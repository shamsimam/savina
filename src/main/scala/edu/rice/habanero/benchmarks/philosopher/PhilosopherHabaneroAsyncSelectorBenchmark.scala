package edu.rice.habanero.benchmarks.philosopher

import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import edu.rice.habanero.actors.HabaneroSelector
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}
import edu.rice.hj.Module0._
import edu.rice.hj.api.{HjRunnable, HjSuspendable}

/**
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object PhilosopherHabaneroAsyncSelectorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new PhilosopherHabaneroAsyncSelectorBenchmark)
  }

  private final class PhilosopherHabaneroAsyncSelectorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      PhilosopherConfig.parseArgs(args)
    }

    def printArgInfo() {
      PhilosopherConfig.printArgs()
    }

    def runIteration() {
      val counter = new AtomicLong(0)

      finish(new HjSuspendable {
        override def run() = {

          val arbitrator = new ArbitratorActor(PhilosopherConfig.N)
          arbitrator.start()

          val philosophers = Array.tabulate[HabaneroSelector[AnyRef]](PhilosopherConfig.N)(i => {
            val loopActor = new PhilosopherActor(i, PhilosopherConfig.M, counter, arbitrator)
            loopActor.start()
            loopActor
          })

          philosophers.foreach(loopActor => {
            loopActor.send(0, StartMessage())
          })
        }
      })

      println("  Num retries: " + counter.get())
      track("Avg. Retry Count", counter.get())
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
    }
  }


  case class StartMessage()

  case class ExitMessage()

  case class HungryMessage(philosopher: HabaneroSelector[AnyRef], philosopherId: Int)

  case class DoneMessage(philosopherId: Int)

  case class EatMessage()

  case class DeniedMessage()


  private class PhilosopherActor(id: Int, rounds: Int, counter: AtomicLong, arbitrator: ArbitratorActor) extends HabaneroSelector[AnyRef](1) {

    private val self = this
    private var localCounter = 0L
    private var roundsSoFar = 0

    private val myHungryMessage = HungryMessage(self, id)
    private val myDoneMessage = DoneMessage(id)

    override def process(msg: AnyRef) {
      msg match {

        case dm: DeniedMessage =>

          localCounter += 1
          arbitrator.send(0, myHungryMessage)

        case em: EatMessage =>

          roundsSoFar += 1
          counter.addAndGet(localCounter)

          arbitrator.send(0, myDoneMessage)
          if (roundsSoFar < rounds) {
            self.send(0, StartMessage())
          } else {
            arbitrator.send(0, ExitMessage())
            exit()
          }

        case sm: StartMessage =>

          arbitrator.send(0, myHungryMessage)

      }
    }
  }

  private class ArbitratorActor(numForks: Int) extends HabaneroSelector[AnyRef](1, false) {

    private val forks = Array.tabulate(numForks)(i => new AtomicBoolean(false))
    private var numExitedPhilosophers = 0

    override def process(msg: AnyRef) {
      msg match {
        case hm: HungryMessage =>

          asyncNb(new HjRunnable {
            override def run(): Unit = {

              val leftFork = forks(hm.philosopherId)
              val leftSuccess = leftFork.compareAndSet(false, true)
              if (!leftSuccess) {
                hm.philosopher.send(0, DeniedMessage())
                return
              }

              val rightFork = forks((hm.philosopherId + 1) % numForks)
              val rightSuccess = rightFork.compareAndSet(false, true)
              if (!rightSuccess) {
                leftFork.compareAndSet(true, false)
                hm.philosopher.send(0, DeniedMessage())
                return
              }

              hm.philosopher.send(0, EatMessage())
            }
          })

        case dm: DoneMessage =>

          val leftFork = forks(dm.philosopherId)
          val rightFork = forks((dm.philosopherId + 1) % numForks)
          leftFork.compareAndSet(true, false)
          rightFork.compareAndSet(true, false)

        case em: ExitMessage =>

          numExitedPhilosophers += 1
          if (numForks == numExitedPhilosophers) {
            exit()
          }
      }
    }
  }

}
