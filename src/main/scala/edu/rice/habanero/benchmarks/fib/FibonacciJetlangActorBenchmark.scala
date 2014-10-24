package edu.rice.habanero.benchmarks.fib

import edu.rice.habanero.actors.{JetlangActor, JetlangActorState, JetlangPool}
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}

/**
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object FibonacciJetlangActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new FibonacciJetlangActorBenchmark)
  }

  private final class FibonacciJetlangActorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      FibonacciConfig.parseArgs(args)
    }

    def printArgInfo() {
      FibonacciConfig.printArgs()
    }

    def runIteration() {

      val fjRunner = new FibonacciActor(null)
      fjRunner.start()
      fjRunner.send(Request(FibonacciConfig.N))

      JetlangActorState.awaitTermination()
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double): Unit = {
      if (lastIteration) {
        JetlangPool.shutdown()
      }
    }
  }

  private case class Request(n: Int)

  private case class Response(value: Int)

  private val RESPONSE_ONE = Response(1)


  private class FibonacciActor(parent: JetlangActor[AnyRef]) extends JetlangActor[AnyRef] {

    private val self = this
    private var result = 0
    private var respReceived = 0

    override def process(msg: AnyRef) {

      msg match {
        case req: Request =>

          if (req.n <= 2) {

            result = 1
            processResult(RESPONSE_ONE)

          } else {

            val f1 = new FibonacciActor(self)
            f1.start()
            f1.send(Request(req.n - 1))

            val f2 = new FibonacciActor(self)
            f2.start()
            f2.send(Request(req.n - 2))

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
        parent.send(response)
      } else {
        println(" Result = " + result)
      }

      exit()
    }
  }

}
