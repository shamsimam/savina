package edu.rice.habanero.benchmarks.fjcreate

import edu.rice.habanero.actors.{GparsActor, GparsActorState, GparsPool}
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}

/**
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object ForkJoinGparsActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new ForkJoinGparsActorBenchmark)
  }

  private final class ForkJoinGparsActorBenchmark extends Benchmark {
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

      GparsActorState.awaitTermination()
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
      if (lastIteration) {
        GparsPool.shutdown()
      }
    }
  }

  private class ForkJoinActor extends GparsActor[AnyRef] {
    override def process(msg: AnyRef) {
      ForkJoinConfig.performComputation(37.2)
      exit()
    }
  }

}
