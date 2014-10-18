package edu.rice.habanero.actors

import java.util.concurrent.atomic.AtomicBoolean

import edu.rice.hj.runtime.config.HjSystemProperty
import edu.rice.hj.runtime.util.ModCountDownLatch
import groovyx.gpars.actor.DefaultActor
import groovyx.gpars.actor.impl.MessageStream
import groovyx.gpars.group.DefaultPGroup
import groovyx.gpars.scheduler.FJPool

/**
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object GparsActorState {

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

object GparsPool {

  val pGroup = {
    val workers: Int = HjSystemProperty.numWorkers.getPropertyValue.toInt
    val fJPool = new FJPool(workers)
    new DefaultPGroup(fJPool)
  }

  def shutdown(): Unit = pGroup.shutdown()
}

abstract class GparsActor[MsgType] extends DefaultActor {

  private final val selfRef = this
  private val startTracker = new AtomicBoolean(false)
  private val exitTracker = new AtomicBoolean(false)
  setParallelGroup(GparsPool.pGroup)


  override def handleException(exception: Throwable): Unit = {
    exception.printStackTrace()
  }

  override def send(message: scala.Any): MessageStream = {
    if (exitTracker.get()) {
      null
    } else {
      super.send(message)
    }
  }

  override def act(): Unit = {
    /**
     * Groovy code:
    loop {
      react { it ->
        process(it)
      }
    }
     */
    loop(new Runnable {
      override def run(): Unit = {
        react(new groovy.lang.Closure[Unit](selfRef) {
          override def call(message: scala.Any): Unit = {
            process(message.asInstanceOf[MsgType])
          }
        })
      }
    })
  }

  def process(msg: MsgType): Unit

  final def hasStarted() = {
    startTracker.get()
  }

  final override def start() = {
    if (!hasStarted()) {
      GparsActorState.actorLatch.updateCount()
      onPreStart()
      super.start()
      onPostStart()
      startTracker.set(true)
    }
    selfRef
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
      stop()
      onPostExit()
      GparsActorState.actorLatch.countDown()
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

