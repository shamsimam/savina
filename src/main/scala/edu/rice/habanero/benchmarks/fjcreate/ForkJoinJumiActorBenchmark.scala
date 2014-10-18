package edu.rice.habanero.benchmarks.fjcreate

import edu.rice.habanero.actors.{JumiActor, JumiActorState, JumiPool}
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}

/**
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object ForkJoinJumiActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new ForkJoinJumiActorBenchmark)
  }

  private final class ForkJoinJumiActorBenchmark extends Benchmark {
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

      JumiActorState.awaitTermination()
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
      if (lastIteration) {
        JumiPool.shutdown()
      }
    }
  }

  private class ForkJoinActor extends JumiActor[AnyRef] {
    override def process(msg: AnyRef) {
      ForkJoinConfig.performComputation(37.2)
      exit()
    }
  }

}
