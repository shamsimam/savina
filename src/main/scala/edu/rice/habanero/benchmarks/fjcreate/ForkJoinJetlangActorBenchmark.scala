package edu.rice.habanero.benchmarks.fjcreate

import edu.rice.habanero.actors.{JetlangActor, JetlangActorState, JetlangPool}
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}

/**
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object ForkJoinJetlangActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new ForkJoinJetlangActorBenchmark)
  }

  private final class ForkJoinJetlangActorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      ForkJoinConfig.parseArgs(args)
    }

    def printArgInfo() {
      ForkJoinConfig.printArgs()
    }

    def runIteration() {
      val message = new Object()
      var i = 0
      while (i < ForkJoinConfig.N) {
        val fjRunner = new ForkJoinActor()
        fjRunner.start()
        fjRunner.send(message)
        i += 1
      }

      JetlangActorState.awaitTermination()
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
      if (lastIteration) {
        JetlangPool.shutdown()
      }
    }
  }

  private class ForkJoinActor extends JetlangActor[AnyRef] {
    override def process(msg: AnyRef) {
      ForkJoinConfig.performComputation(37.2)
      exit()
    }
  }

}
