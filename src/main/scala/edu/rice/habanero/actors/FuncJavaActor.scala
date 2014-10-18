package edu.rice.habanero.actors

import java.util.concurrent.ForkJoinPool.ManagedBlocker
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{ConcurrentLinkedQueue, ForkJoinPool}

import edu.rice.hj.runtime.config.HjSystemProperty
import edu.rice.hj.runtime.util.ModCountDownLatch
import fj.Effect
import fj.control.parallel.{Actor, Strategy}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Promise}

/**
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object FuncJavaActorState {

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

object FuncJavaPool {

  var executors = {
    val workers: Int = HjSystemProperty.numWorkers.getPropertyValue.toInt
    new ForkJoinPool(workers)
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

abstract class FuncJavaActor[MsgType] {

  private val startTracker = new AtomicBoolean(false)
  private val exitTracker = new AtomicBoolean(false)

  // Lock to ensure the actor only acts on one message at a time
  private val suspended = new AtomicBoolean(false)

  // Queue to hold pending messages
  private val mbox = new ConcurrentLinkedQueue[MsgType]()

  private val strategy = Strategy.executorStrategy[fj.Unit](FuncJavaPool.executors)
  private val effect = new Effect[MsgType] {
    override def e(msg: MsgType): Unit = receive(msg)
  }

  // Product so the actor can use its strategy (to act on messages in other threads, to handle exceptions, etc.)
  private val processor: fj.P1[fj.Unit] = new fj.P1[fj.Unit]() {
    def _1(): fj.Unit = {
      // get next item from queue
      val a: MsgType = mbox.poll()
      // if there is one, process it
      if (a != null) {
        effect.e(a)
        // try again, in case there are more messages
        strategy.par(this)
      } else {
        // clear the lock
        suspended.set(true)
        // work again, in case someone else queued up a message while we were holding the lock
        work()
      }
      fj.Unit.unit()
    }
  }

  // The FunctionJava queued actor to which the messages are delegated
  private val fjActor = {

    // inspired from fj.control.parallel.Actor#queueActor() method
    Actor.actor[MsgType](Strategy.seqStrategy[fj.Unit](), new Effect[MsgType]() {

      // Effect's body -- queues up a message and tries to un-suspend the actor
      override def e(msg: MsgType): Unit = {
        mbox.offer(msg)
        work()
      }
    })
  }

  // If there are pending messages, use the strategy to run the processor
  def work(): Unit = {
    if (!mbox.isEmpty && suspended.compareAndSet(true, false)) {
      strategy.par(processor)
    }
  }

  final def receive(msg: MsgType): Unit = {
    if (!exitTracker.get()) {
      process(msg)
    }
  }

  def process(msg: MsgType): Unit

  def send(msg: MsgType) {
    fjActor.act(msg)
  }

  final def hasStarted() = {
    startTracker.get()
  }

  final def start() = {
    if (!hasStarted()) {
      FuncJavaActorState.actorLatch.updateCount()
      onPreStart()
      onPostStart()
      startTracker.set(true)

      // allow fjActor to start acting on messages
      suspended.set(true)
      work()
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
      FuncJavaActorState.actorLatch.countDown()
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
