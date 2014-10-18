package edu.rice.habanero.actors

import java.util.concurrent.atomic.AtomicBoolean

import edu.rice.hj.runtime.config.HjSystemProperty
import edu.rice.hj.runtime.util.ModCountDownLatch
import net.liftweb.actor.LAScheduler

/**
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object LiftActorState {

  val workers: Int = HjSystemProperty.numWorkers.getPropertyValue.toInt
  LAScheduler.threadPoolSize = workers
  LAScheduler.maxThreadPoolSize = workers

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

object LiftPool {

  def shutdown(): Unit = LAScheduler.shutdown()
}

class LiftActor[MsgType] {

  private val startTracker = new AtomicBoolean(false)
  private val exitTracker = new AtomicBoolean(false)
  private val wrappedActor = new net.liftweb.actor.LiftActor() {
    override protected def messageHandler = {
      case msg: Any =>
        process(msg.asInstanceOf[MsgType])
    }
  }

  def process(msg: MsgType): Unit = {
    throw new IllegalStateException("Must be overridden in child class")
  }

  final def send(msg: MsgType) {
    if (!hasExited()) {
      wrappedActor.send(msg)
    }
  }

  final def hasStarted() = {
    startTracker.get()
  }

  final def start() = {
    if (!hasStarted()) {
      LiftActorState.actorLatch.updateCount()
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
      LiftActorState.actorLatch.countDown()
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
