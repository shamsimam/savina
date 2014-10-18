package edu.rice.habanero.benchmarks.count

import akka.actor.{ActorRef, Props}
import edu.rice.habanero.actors.{AkkaActor, AkkaActorState}
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}

/**
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object CountingAkkaActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new CountingAkkaActorBenchmark)
  }

  private final class CountingAkkaActorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      CountingConfig.parseArgs(args)
    }

    def printArgInfo() {
      CountingConfig.printArgs()
    }

    def runIteration() {

      val system = AkkaActorState.newActorSystem("Counting")

      val counter = system.actorOf(Props(new CountingActor()))
      AkkaActorState.startActor(counter)

      val producer = system.actorOf(Props(new ProducerActor(counter)))
      AkkaActorState.startActor(producer)

      producer ! IncrementMessage()

      AkkaActorState.awaitTermination(system)
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
    }
  }

  private case class IncrementMessage()

  private case class RetrieveMessage(sender: ActorRef)

  private case class ResultMessage(result: Int)

  private class ProducerActor(counter: ActorRef) extends AkkaActor[AnyRef] {

    override def process(msg: AnyRef) {
      msg match {
        case m: IncrementMessage =>

          var i = 0
          while (i < CountingConfig.N) {
            counter ! m
            i += 1
          }

          counter ! RetrieveMessage(self)

        case m: ResultMessage =>
          val result = m.result
          if (result != CountingConfig.N) {
            println("ERROR: expected: " + CountingConfig.N + ", found: " + result)
          } else {
            println("SUCCESS! received: " + result)
          }
          exit()
      }
    }
  }

  private class CountingActor extends AkkaActor[AnyRef] {

    private var count = 0

    override def process(msg: AnyRef) {
      msg match {
        case m: IncrementMessage =>
          count += 1
        case m: RetrieveMessage =>
          m.sender ! ResultMessage(count)
          exit()
      }
    }
  }

}
