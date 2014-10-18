package edu.rice.habanero.benchmarks.philosopher

import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import edu.rice.habanero.actors.{JumiActor, JumiActorState, JumiPool}
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}

/**
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object PhilosopherJumiActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new PhilosopherJumiActorBenchmark)
  }

  private final class PhilosopherJumiActorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      PhilosopherConfig.parseArgs(args)
    }

    def printArgInfo() {
      PhilosopherConfig.printArgs()
    }

    def runIteration() {
      val counter = new AtomicLong(0)


      val arbitrator = new ArbitratorActor(PhilosopherConfig.N)
      arbitrator.start()

      val philosophers = Array.tabulate[JumiActor[AnyRef]](PhilosopherConfig.N)(i => {
        val loopActor = new PhilosopherActor(i, PhilosopherConfig.M, counter, arbitrator)
        loopActor.start()
        loopActor
      })

      philosophers.foreach(loopActor => {
        loopActor.send(StartMessage())
      })

      JumiActorState.awaitTermination()

      println("  Num retries: " + counter.get())
      track("Avg. Retry Count", counter.get())
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double): Unit = {
      if (lastIteration) {
        JumiPool.shutdown()
      }
    }
  }


  case class StartMessage()

  case class ExitMessage()

  case class HungryMessage(philosopher: JumiActor[AnyRef], philosopherId: Int)

  case class DoneMessage(philosopherId: Int)

  case class EatMessage()

  case class DeniedMessage()


  private class PhilosopherActor(id: Int, rounds: Int, counter: AtomicLong, arbitrator: ArbitratorActor) extends JumiActor[AnyRef] {

    private val self = this
    private var localCounter = 0L
    private var roundsSoFar = 0

    private val myHungryMessage = HungryMessage(self, id)
    private val myDoneMessage = DoneMessage(id)

    override def process(msg: AnyRef) {
      msg match {

        case dm: DeniedMessage =>

          localCounter += 1
          arbitrator.send(myHungryMessage)

        case em: EatMessage =>

          roundsSoFar += 1
          counter.addAndGet(localCounter)

          arbitrator.send(myDoneMessage)
          if (roundsSoFar < rounds) {
            self.send(StartMessage())
          } else {
            arbitrator.send(ExitMessage())
            exit()
          }

        case sm: StartMessage =>

          arbitrator.send(myHungryMessage)

      }
    }
  }

  private class ArbitratorActor(numForks: Int) extends JumiActor[AnyRef] {

    private val forks = Array.tabulate(numForks)(i => new AtomicBoolean(false))
    private var numExitedPhilosophers = 0

    override def process(msg: AnyRef) {
      msg match {
        case hm: HungryMessage =>

          val leftFork = forks(hm.philosopherId)
          val rightFork = forks((hm.philosopherId + 1) % numForks)

          if (leftFork.get() || rightFork.get()) {
            // someone else has access to the fork
            hm.philosopher.send(DeniedMessage())
          } else {
            leftFork.set(true)
            rightFork.set(true)
            hm.philosopher.send(EatMessage())
          }

        case dm: DoneMessage =>

          val leftFork = forks(dm.philosopherId)
          val rightFork = forks((dm.philosopherId + 1) % numForks)
          leftFork.set(false)
          rightFork.set(false)

        case em: ExitMessage =>

          numExitedPhilosophers += 1
          if (numForks == numExitedPhilosophers) {
            exit()
          }
      }
    }
  }

}
