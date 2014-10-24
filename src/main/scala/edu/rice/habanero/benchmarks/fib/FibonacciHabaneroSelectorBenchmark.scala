package edu.rice.habanero.benchmarks.fib

import edu.rice.habanero.actors.HabaneroSelector
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}
import edu.rice.hj.Module0._
import edu.rice.hj.api.HjSuspendable

/**
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object FibonacciHabaneroSelectorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new FibonacciHabaneroSelectorBenchmark)
  }

  private final class FibonacciHabaneroSelectorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      FibonacciConfig.parseArgs(args)
    }

    def printArgInfo() {
      FibonacciConfig.printArgs()
    }

    def runIteration() {
      finish(new HjSuspendable {
        override def run() = {

          val fjRunner = new FibonacciSelector(null)
          fjRunner.start()
          fjRunner.send(0, Request(FibonacciConfig.N))

        }
      })
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
    }
  }

  private case class Request(n: Int)

  private case class Response(value: Int)

  private val RESPONSE_ONE = Response(1)


  private class FibonacciSelector(parent: HabaneroSelector[AnyRef]) extends HabaneroSelector[AnyRef](1) {

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

            val f1 = new FibonacciSelector(self)
            f1.start()
            f1.send(0, Request(req.n - 1))

            val f2 = new FibonacciSelector(self)
            f2.start()
            f2.send(0, Request(req.n - 2))

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
        parent.send(0, response)
      } else {
        println(" Result = " + result)
      }

      exit()
    }
  }

}
