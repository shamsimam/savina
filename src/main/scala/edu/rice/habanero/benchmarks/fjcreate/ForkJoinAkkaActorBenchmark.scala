package edu.rice.habanero.benchmarks.fjcreate

import akka.actor.Props
import edu.rice.habanero.actors.{AkkaActor, AkkaActorState}
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}

/**
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object ForkJoinAkkaActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new ForkJoinAkkaActorBenchmark)
  }

  private final class ForkJoinAkkaActorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      ForkJoinConfig.parseArgs(args)
    }

    def printArgInfo() {
      ForkJoinConfig.printArgs()
    }

    def runIteration() {

      val system = AkkaActorState.newActorSystem("ForkJoin")

      val message = new Object()
      var i = 0
      while (i < ForkJoinConfig.N) {
        val fjRunner = system.actorOf(Props(new ForkJoinActor()))
        AkkaActorState.startActor(fjRunner)
        fjRunner ! message
        i += 1
      }

      AkkaActorState.awaitTermination(system)
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
    }
  }

  private class ForkJoinActor extends AkkaActor[AnyRef] {
    override def process(msg: AnyRef) {
      start()
      ForkJoinConfig.performComputation(37.2)
      exit()
    }
  }

}
