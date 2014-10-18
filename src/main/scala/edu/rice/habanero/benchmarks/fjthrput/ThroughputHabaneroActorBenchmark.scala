package edu.rice.habanero.benchmarks.fjthrput

import edu.rice.habanero.actors.HabaneroActor
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}
import edu.rice.hj.Module0._
import edu.rice.hj.api.HjSuspendable

/**
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object ThroughputHabaneroActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new ThroughputHabaneroActorBenchmark)
  }

  private final class ThroughputHabaneroActorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      ThroughputConfig.parseArgs(args)
    }

    def printArgInfo() {
      ThroughputConfig.printArgs()
    }

    def runIteration() {
      finish(new HjSuspendable {
        override def run() = {

          val actors = Array.tabulate[ThroughputActor](ThroughputConfig.A)(i => {
            val loopActor = new ThroughputActor(ThroughputConfig.N)
            loopActor.start()
            loopActor
          })

          val message = new Object()

          var m = 0
          while (m < ThroughputConfig.N) {

            actors.foreach(loopActor => {
              loopActor.send(message)
            })

            m += 1
          }

        }
      })
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
    }
  }

  private class ThroughputActor(totalMessages: Int) extends HabaneroActor[AnyRef] {

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
