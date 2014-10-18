package edu.rice.habanero.benchmarks.fjthrput

import edu.rice.habanero.actors.{JetlangActor, JetlangActorState, JetlangPool}
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}

/**
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object ThroughputJetlangActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new ThroughputJetlangActorBenchmark)
  }

  private final class ThroughputJetlangActorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      ThroughputConfig.parseArgs(args)
    }

    def printArgInfo() {
      ThroughputConfig.printArgs()
    }

    def runIteration() {

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

      JetlangActorState.awaitTermination()
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
      if (lastIteration) {
        JetlangPool.shutdown()
      }
    }
  }

  private class ThroughputActor(totalMessages: Int) extends JetlangActor[AnyRef] {

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
