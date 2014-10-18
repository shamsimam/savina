package edu.rice.habanero.benchmarks.fjcreate

import edu.rice.habanero.actors.HabaneroSelector
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}
import edu.rice.hj.Module0._
import edu.rice.hj.api.HjSuspendable

/**
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object ForkJoinHabaneroSelectorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new ForkJoinHabaneroSelectorBenchmark)
  }

  private final class ForkJoinHabaneroSelectorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      ForkJoinConfig.parseArgs(args)
    }

    def printArgInfo() {
      ForkJoinConfig.printArgs()
    }

    def runIteration() {
      finish(new HjSuspendable {
        override def run() = {
          val message = new Object()
          var i = 0
          while (i < ForkJoinConfig.N) {
            val fjRunner = new ForkJoinSelector()
            fjRunner.start()
            fjRunner.send(i % ForkJoinConfig.C, message)
            i += 1
          }
        }
      })
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
    }
  }

  private class ForkJoinSelector extends HabaneroSelector[AnyRef](ForkJoinConfig.C) {
    override def process(msg: AnyRef) {
      ForkJoinConfig.performComputation(37.2)
      exit()
    }
  }


}
