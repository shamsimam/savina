package edu.rice.habanero.benchmarks.count

import edu.rice.habanero.actors.HabaneroSelector
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}
import edu.rice.hj.Module0._
import edu.rice.hj.api.HjSuspendable

/**
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object CountingHabaneroSelectorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new CountingHabaneroSelectorBenchmark)
  }

  private final class CountingHabaneroSelectorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      CountingConfig.parseArgs(args)
    }

    def printArgInfo() {
      CountingConfig.printArgs()
    }

    def runIteration() {
      finish(new HjSuspendable {
        override def run() = {

          val counter = new CountingActor()
          counter.start()

          val producer = new ProducerActor(counter)
          producer.start()

          producer.send(0, IncrementMessage())
        }
      })
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
    }
  }

  private case class IncrementMessage()

  private case class RetrieveMessage(sender: HabaneroSelector[AnyRef])

  private case class ResultMessage(result: Int)

  private class ProducerActor(counter: HabaneroSelector[AnyRef]) extends HabaneroSelector[AnyRef](1) {

    private val self = this

    override def process(msg: AnyRef) {
      msg match {
        case m: IncrementMessage =>

          var i = 0
          while (i < CountingConfig.N) {
            counter.send(0, m)
            i += 1
          }

          counter.send(0, RetrieveMessage(self))

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

  private class CountingActor extends HabaneroSelector[AnyRef](1) {

    private var count = 0

    override def process(msg: AnyRef) {
      msg match {
        case m: IncrementMessage =>
          count += 1
        case m: RetrieveMessage =>
          m.sender.send(0, ResultMessage(count))
          exit()
      }
    }
  }

}
