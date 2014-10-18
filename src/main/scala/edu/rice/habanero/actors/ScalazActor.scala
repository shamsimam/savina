package edu.rice.habanero.actors

import java.util.concurrent.ForkJoinPool
import java.util.concurrent.ForkJoinPool.ManagedBlocker
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import edu.rice.hj.runtime.config.HjSystemProperty
import edu.rice.hj.runtime.util.ModCountDownLatch

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Promise}
import scalaz.concurrent.{Actor, Strategy}

/**
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object ScalazActorState {
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

object ScalazPool {

  val executors: ForkJoinPool = {
    val workers: Int = HjSystemProperty.numWorkers.getPropertyValue.toInt
    new ForkJoinPool(workers)
  }

  /**
   * A strategy that evaluates its arguments using an implicit ExecutorService.
   */
  def fjStrategy(counter: AtomicInteger): Strategy = {
    new Strategy {

      import java.util.concurrent.Callable

      def apply[A](a: => A) = {
        counter.incrementAndGet()
        val fut = executors.submit(new Callable[A] {
          def call = a
        })
        () => fut.get
      }
    }
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

abstract class ScalazActor[MsgType] {

  private val startTracker = new AtomicBoolean(false)
  private val exitTracker = new AtomicBoolean(false)

  private val taskCounter: AtomicInteger = new AtomicInteger()
  private val messagesSent: AtomicInteger = new AtomicInteger()
  private val messagesProc: AtomicInteger = new AtomicInteger()

  private val scalazActor = {
    Actor[MsgType](msg => {
      if (!exitTracker.get()) {
        messagesProc.incrementAndGet()
        process(msg)
      }
    })(ScalazPool.fjStrategy(taskCounter))
  }

  def process(msg: MsgType): Unit = {
    throw new IllegalStateException("Must be overridden in child class")
  }

  def send(msg: MsgType) {
    if (!exitTracker.get()) {
      messagesSent.incrementAndGet()
      scalazActor(msg)
    }
  }

  final def hasStarted() = {
    startTracker.get()
  }

  final def start() = {
    if (!hasStarted()) {
      ScalazActorState.actorLatch.updateCount()
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
      if (messagesSent.get() != messagesProc.get()) {
        // printActorInfo()
      }
      ScalazActorState.actorLatch.countDown()
    }
  }


  def printActorInfo() {
    println(Thread.currentThread().getName + " ::: " + getClass.getSimpleName +
      ":: sent: " + messagesSent.get() +
      ", processed: " + messagesProc.get() +
      " using " + taskCounter.get() + " tasks.")
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
