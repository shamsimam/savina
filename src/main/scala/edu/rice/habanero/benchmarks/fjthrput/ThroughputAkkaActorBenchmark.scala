package edu.rice.habanero.benchmarks.fjthrput

import akka.actor.{ActorRef, Props}
import edu.rice.habanero.actors.{AkkaActor, AkkaActorState}
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}

/**
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object ThroughputAkkaActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new ThroughputAkkaActorBenchmark)
  }

  private final class ThroughputAkkaActorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      ThroughputConfig.parseArgs(args)
    }

    def printArgInfo() {
      ThroughputConfig.printArgs()
    }

    def runIteration() {

      val system = AkkaActorState.newActorSystem("Throughput")

      val actors = Array.tabulate[ActorRef](ThroughputConfig.A)(i => {
        val loopActor = system.actorOf(Props(new ThroughputActor(ThroughputConfig.N)))
        AkkaActorState.startActor(loopActor)
        loopActor
      })

      val message = new Object()

      var m = 0
      while (m < ThroughputConfig.N) {

        actors.foreach(loopActor => {
          loopActor ! message
        })

        m += 1
      }

      AkkaActorState.awaitTermination(system)
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
    }
  }

  private class ThroughputActor(totalMessages: Int) extends AkkaActor[AnyRef] {

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
