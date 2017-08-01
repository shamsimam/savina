package edu.rice.habanero.benchmarks.cigsmok

import edu.rice.habanero.actors.HabaneroSelector
import edu.rice.habanero.benchmarks.cigsmok.CigaretteSmokerConfig.{ExitMessage, StartMessage, StartSmoking, StartedSmoking}
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner, PseudoRandom}
import edu.rice.hj.Module0._
import edu.rice.hj.api.HjSuspendable

/**
 * Based on solution in <a href="http://en.wikipedia.org/wiki/Cigarette_smokers_problem">Wikipedia</a> where resources are acquired instantaneously.
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object CigaretteSmokerHabaneroSelectorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new CigaretteSmokerHabaneroSelectorBenchmark)
  }

  private final class CigaretteSmokerHabaneroSelectorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      CigaretteSmokerConfig.parseArgs(args)
    }

    def printArgInfo() {
      CigaretteSmokerConfig.printArgs()
    }

    def runIteration() {
      finish(new HjSuspendable {
        override def run() = {

          val arbiterSelector = new ArbiterSelector(CigaretteSmokerConfig.R, CigaretteSmokerConfig.S)
          arbiterSelector.start()

          arbiterSelector.send(0, StartMessage.ONLY)
        }
      })
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
    }
  }

  private class ArbiterSelector(numRounds: Int, numSmokers: Int) extends HabaneroSelector[AnyRef](1) {

    private val self = this
    private val smokerSelectors = Array.tabulate[HabaneroSelector[AnyRef]](numSmokers)(i => new SmokerSelector(self))
    private val random = new PseudoRandom(numRounds * numSmokers)
    private var roundsSoFar = 0

    override def onPostStart() {
      smokerSelectors.foreach(loopSelector => {
        loopSelector.start()
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
      smokerSelectors(newSmokerIndex).send(0, new StartSmoking(busyWaitPeriod))
    }

    private def requestSmokersToExit() {
      smokerSelectors.foreach(loopSelector => {
        loopSelector.send(0, ExitMessage.ONLY)
      })
    }
  }

  private class SmokerSelector(arbiterSelector: ArbiterSelector) extends HabaneroSelector[AnyRef](1) {
    override def process(msg: AnyRef) {
      msg match {
        case sm: StartSmoking =>

          // notify arbiter that started smoking
          arbiterSelector.send(0, StartedSmoking.ONLY)
          // now smoke cigarette
          CigaretteSmokerConfig.busyWait(sm.busyWaitPeriod)

        case em: ExitMessage =>

          exit()
      }
    }
  }

}
