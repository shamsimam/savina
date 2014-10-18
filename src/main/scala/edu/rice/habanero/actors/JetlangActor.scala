package edu.rice.habanero.actors

import java.util.concurrent.ForkJoinPool
import java.util.concurrent.ForkJoinPool.ManagedBlocker
import java.util.concurrent.atomic.AtomicBoolean

import edu.rice.hj.runtime.config.HjSystemProperty
import edu.rice.hj.runtime.util.ModCountDownLatch
import org.jetlang.core.{BatchExecutor, EventReader}
import org.jetlang.fibers.{Fiber, PoolFiberFactory}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Promise}

/**
 * ORIGINAL SOURCE: http://code.google.com/p/jetlang/source/browse/scala/src/jetlang/example/JetlangActor.scala
 * March 16, 2012
 *
 * Note:
 * - Fixed ActorExecutor signature to use  execute(EventReader) instead of execute(Array[Runnable])
 * - allow configurable pool size using system property: actors.corePoolSize
 * - add start() and exit() to JetlangActor
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */

object JetlangActorState {
  val actorLatch = new ModCountDownLatch(0)
  val current = new ThreadLocal[ReplyTo[_]]()

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

class ReplyTo[MsgType](actor: MsgType => Unit) {
  def !(msg: MsgType): Unit = actor(msg)
}

class ActorExecutor[MsgType](actor: MsgType => Unit) extends BatchExecutor {
  val replyTo = new ReplyTo[MsgType](actor)

  def execute(eventReader: EventReader) = {
    JetlangActorState.current.set(replyTo)
    for (index <- 0 to eventReader.size() - 1)
      eventReader.get(index).run()
    JetlangActorState.current.set(null)
  }
}

object JetlangPool {

  val executors = {
    val workers: Int = HjSystemProperty.numWorkers.getPropertyValue.toInt
    new ForkJoinPool(workers)
  }
  private val fiberFactory = new PoolFiberFactory(executors)

  def create[MsgType](callback: MsgType => Unit): Fiber = {
    val e = new ActorExecutor[MsgType](callback)
    fiberFactory.create(e)
  }

  def await[A](aPromise: Promise[A]): A = {
    val blocker = new ManagedBlocker {
      override def block(): Boolean = {
        Await.result(aPromise.future, Duration.Inf)
        true
      }

      override def isReleasable(): Boolean = {
        aPromise.isCompleted
      }
    }
    ForkJoinPool.managedBlock(blocker)
    val res = Await.result(aPromise.future, Duration.Inf)
    res
  }

  def shutdown(): Unit = executors.shutdown()
}

abstract class JetlangActor[MsgType] {

  private val startTracker = new AtomicBoolean(false)
  private val exitTracker = new AtomicBoolean(false)

  val fiber = createFiber(receiveMsg)
  var sender: ReplyTo[MsgType] = null

  def createFiber(callback: MsgType => Unit): Fiber = JetlangPool.create(callback)

  final def receiveMsg(msg: MsgType): Unit = {
    val runner = new Runnable() {
      def run() = {
        process(msg)
      }
    }
    try {
      fiber.execute(runner)
    } catch {
      case th: Throwable =>
        th.printStackTrace(System.err)
        System.err.flush()
        th.getCause
    }
  }

  final def !(msg: MsgType): Unit = receiveMsg(msg)

  final def send(msg: MsgType): Unit = receiveMsg(msg)

  def process(msg: MsgType): Unit

  final def hasStarted() = {
    startTracker.get()
  }

  final def start() = {
    if (!hasStarted()) {
      JetlangActorState.actorLatch.updateCount()
      onPreStart()
      fiber.start()
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
      fiber.dispose()
      onPostExit()
      JetlangActorState.actorLatch.countDown()
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
