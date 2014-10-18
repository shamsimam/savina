package edu.rice.habanero.actors

import java.util.concurrent.atomic.AtomicBoolean

import edu.rice.hj.runtime.util.ModCountDownLatch

import scala.actors.Actor

/**
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object ScalaActorState {
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

class ScalaActor[MsgType] extends Actor {

  private val startTracker = new AtomicBoolean(false)
  private val exitTracker = new AtomicBoolean(false)

  final def act() {
    loop {
      react {
        case msg: Any =>
          process(msg.asInstanceOf[MsgType])
      }
    }
  }

  def process(msg: MsgType): Unit = {
    throw new IllegalStateException("Must be overridden in child class")
  }

  def send(msg: MsgType) {
    this ! msg
  }

  final def hasStarted() = {
    startTracker.get()
  }

  override final def start() = {
    if (!hasStarted()) {
      ScalaActorState.actorLatch.updateCount()
      onPreStart()
      val res = super.start()
      onPostStart()
      startTracker.set(true)
      res
    }
    this
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

  override final def exit() = {
    val success = exitTracker.compareAndSet(false, true)
    if (success) {
      onPreExit()
      onPostExit()
      ScalaActorState.actorLatch.countDown()
    }
    super.exit()
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
