package edu.rice.habanero.benchmarks.cigsmok

import edu.rice.habanero.actors.HabaneroActor
import edu.rice.habanero.benchmarks.cigsmok.CigaretteSmokerConfig.{ExitMessage, StartMessage, StartSmoking, StartedSmoking}
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner, PseudoRandom}
import edu.rice.hj.Module0._
import edu.rice.hj.api.HjSuspendable

/**
 * Based on solution in <a href="http://en.wikipedia.org/wiki/Cigarette_smokers_problem">Wikipedia</a> where resources are acquired instantaneously.
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object CigaretteSmokerHabaneroActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new CigaretteSmokerHabaneroActorBenchmark)
  }

  private final class CigaretteSmokerHabaneroActorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      CigaretteSmokerConfig.parseArgs(args)
    }

    def printArgInfo() {
      CigaretteSmokerConfig.printArgs()
    }

    def runIteration() {
      finish(new HjSuspendable {
        override def run() = {

          val arbiterActor = new ArbiterActor(CigaretteSmokerConfig.R, CigaretteSmokerConfig.S)
          arbiterActor.start()

          arbiterActor.send(StartMessage.ONLY)
        }
      })
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
    }
  }

  private class ArbiterActor(numRounds: Int, numSmokers: Int) extends HabaneroActor[AnyRef] {

    private val self = this
    private val smokerActors = Array.tabulate[HabaneroActor[AnyRef]](numSmokers)(i => new SmokerActor(self))
    private val random = new PseudoRandom(numRounds * numSmokers)
    private var roundsSoFar = 0

    override def onPostStart() {
      smokerActors.foreach(loopActor => {
        loopActor.start()
      })
    }

    override def process(msg: AnyRef) {
      msg match {
        case sm: StartMessage =>

          // choose a random smoker to start smoking
          notifyRandomSmoker()

        case sm: StartedSmoking =>

          // resources are off the table, can place new ones on the table
          roundsSoFar += 1
          if (roundsSoFar >= numRounds) {
            // had enough, now exit
            requestSmokersToExit()
            exit()
          } else {
            // choose a random smoker to start smoking
            notifyRandomSmoker()
          }
      }
    }

    private def notifyRandomSmoker() {
      // assume resources grabbed instantaneously
      val newSmokerIndex = Math.abs(random.nextInt()) % numSmokers
      val busyWaitPeriod = random.nextInt(1000) + 10
      smokerActors(newSmokerIndex).send(new StartSmoking(busyWaitPeriod))
    }

    private def requestSmokersToExit() {
      smokerActors.foreach(loopActor => {
        loopActor.send(ExitMessage.ONLY)
      })
    }
  }

  private class SmokerActor(arbiterActor: ArbiterActor) extends HabaneroActor[AnyRef] {
    override def process(msg: AnyRef) {
      msg match {
        case sm: StartSmoking =>

          // notify arbiter that started smoking
          arbiterActor.send(StartedSmoking.ONLY)
          // now smoke cigarette
          CigaretteSmokerConfig.busyWait(sm.busyWaitPeriod)

        case em: ExitMessage =>

          exit()
      }
    }
  }

}
