package edu.rice.habanero.benchmarks.fib

import akka.actor.{ActorRef, Props}
import edu.rice.habanero.actors.{AkkaActor, AkkaActorState}
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}

/**
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object FibonacciAkkaActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new FibonacciAkkaActorBenchmark)
  }

  private final class FibonacciAkkaActorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      FibonacciConfig.parseArgs(args)
    }

    def printArgInfo() {
      FibonacciConfig.printArgs()
    }

    def runIteration() {

      val system = AkkaActorState.newActorSystem("Fibonacci")

      val fjRunner = system.actorOf(Props(new FibonacciActor(null)))
      AkkaActorState.startActor(fjRunner)
      fjRunner ! Request(FibonacciConfig.N)

      AkkaActorState.awaitTermination(system)
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
    }
  }

  private case class Request(n: Int)

  private case class Response(value: Int)

  private val RESPONSE_ONE = Response(1)


  private class FibonacciActor(parent: ActorRef) extends AkkaActor[AnyRef] {

    private var result = 0
    private var respReceived = 0

    override def process(msg: AnyRef) {

      msg match {
        case req: Request =>

          if (req.n <= 2) {

            result = 1
            processResult(RESPONSE_ONE)

          } else {

            val f1 = context.system.actorOf(Props(new FibonacciActor(self)))
            AkkaActorState.startActor(f1)
            f1 ! Request(req.n - 1)

            val f2 = context.system.actorOf(Props(new FibonacciActor(self)))
            AkkaActorState.startActor(f2)
            f2 ! Request(req.n - 2)

          }

        case resp: Response =>

          respReceived += 1
          result += resp.value

          if (respReceived == 2) {
            processResult(Response(result))
          }
      }
    }

    private def processResult(response: Response) {
      if (parent != null) {
        parent ! response
      } else {
        println(" Result = " + result)
      }

      exit()
    }
  }

}
