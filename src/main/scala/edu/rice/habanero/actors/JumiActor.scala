package edu.rice.habanero.actors

import java.util.concurrent.ForkJoinPool
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import edu.rice.hj.runtime.config.HjSystemProperty
import edu.rice.hj.runtime.util.ModCountDownLatch
import fi.jumi.actors.eventizers.dynamic.DynamicEventizerProvider
import fi.jumi.actors.listeners.{CrashEarlyFailureHandler, NullMessageListener}
import fi.jumi.actors.{ActorRef, ActorThread, Actors, MultiThreadedActors}

/**
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object JumiActorState {

  val actorLatch = new ModCountDownLatch(0)

  def awaitTermination() {
    try {
      actorLatch.await()
    } catch {
      case ex: InterruptedException => {
        ex.printStackTrace()
      }
    }
  }
}

object JumiPool {

  private val workers: Int = HjSystemProperty.numWorkers.getPropertyValue.toInt
  private val executors: ForkJoinPool = {
    new ForkJoinPool(workers)
  }
  private val actors: Actors = new MultiThreadedActors(
    executors,
    new DynamicEventizerProvider(),
    new CrashEarlyFailureHandler(),
    new NullMessageListener()
  )
  private val actorThreads = Array.tabulate[ActorThread](workers)(i => {
    actors.startActorThread()
  })
  private val actorCounter = new AtomicInteger(-1)

  def actorThread(): ActorThread = {
    val index = actorCounter.incrementAndGet()
    actorThread(index)
  }

  def actorThread(index: Int): ActorThread = {
    actorThreads(index % workers)
  }

  def shutdown(): Unit = executors.shutdown()
}

trait JumiActorImpl[MsgType] {
  def processMessage(msg: MsgType): Unit
}

class JumiActor[MsgType] {

  private val startTracker = new AtomicBoolean(false)
  private val exitTracker = new AtomicBoolean(false)
  private val wrappedActorRef: ActorRef[JumiActorImpl[MsgType]] = JumiPool.actorThread().bindActor(
    classOf[JumiActorImpl[MsgType]], new JumiActorImpl[MsgType] {
      override def processMessage(msg: MsgType): Unit = {
        process(msg.asInstanceOf[MsgType])
      }
    }
  )

  def process(msg: MsgType): Unit = {
    throw new IllegalStateException("Must be overridden in child class")
  }

  final def send(msg: MsgType) {
    if (!hasExited()) {
      wrappedActorRef.tell().processMessage(msg)
    }
  }

  final def hasStarted() = {
    startTracker.get()
  }

  final def start() = {
    if (!hasStarted()) {
      JumiActorState.actorLatch.updateCount()
      onPreStart()
      onPostStart()
      startTracker.set(true)
    }
  }

  /**
   * Convenience: specify code to be executed before actor is started
   */
  protected def onPreStart() = {
  }

  /**
   * Convenience: specify code to be executed after actor is started
   */
  protected def onPostStart() = {
  }

  final def hasExited() = {
    exitTracker.get()
  }

  final def exit() = {
    val success = exitTracker.compareAndSet(false, true)
    if (success) {
      onPreExit()
      onPostExit()
      JumiActorState.actorLatch.countDown()
    }
  }

  /**
   * Convenience: specify code to be executed before actor is terminated
   */
  protected def onPreExit() = {
  }

  /**
   * Convenience: specify code to be executed after actor is terminated
   */
  protected def onPostExit() = {
  }
}
