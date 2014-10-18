package edu.rice.habanero.benchmarks.fjthrput

import edu.rice.habanero.actors.HabaneroSelector
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}
import edu.rice.hj.Module0._
import edu.rice.hj.api.HjSuspendable

/**
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object ThroughputHabaneroSelectorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new ThroughputHabaneroSelectorBenchmark)
  }

  private final class ThroughputHabaneroSelectorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      ThroughputConfig.parseArgs(args)
    }

    def printArgInfo() {
      ThroughputConfig.printArgs()
    }

    def runIteration() {
      finish(new HjSuspendable {
        override def run() = {

          val actors = Array.tabulate[ThroughputSelector](ThroughputConfig.A)(i => {
            val loopSelector = new ThroughputSelector(ThroughputConfig.N)
            loopSelector.start()
            loopSelector
          })

          val message = new Object()
          val numChannels: Int = ThroughputConfig.C

          var m = 0
          while (m < ThroughputConfig.N) {

            actors.foreach(loopSelector => {
              loopSelector.send(m % numChannels, message)
            })

            m += 1
          }

        }
      })
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
    }
  }

  private class ThroughputSelector(totalMessages: Int) extends HabaneroSelector[AnyRef](ThroughputConfig.C, true, ThroughputConfig.usePriorities) {

    private var messagesProcessed = 0

    override def process(msg: AnyRef) {

      messagesProcessed += 1
      ThroughputConfig.performComputation(37.2)

      if (messagesProcessed == totalMessages) {
        exit()
      }
    }
  }

}
