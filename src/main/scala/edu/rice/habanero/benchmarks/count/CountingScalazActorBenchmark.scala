package edu.rice.habanero.benchmarks.count

import edu.rice.habanero.actors.{ScalazActor, ScalazActorState, ScalazPool}
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}

/**
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object CountingScalazActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new CountingScalazActorBenchmark)
  }

  private final class CountingScalazActorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      CountingConfig.parseArgs(args)
    }

    def printArgInfo() {
      CountingConfig.printArgs()
    }

    def runIteration() {

      val counter = new CountingActor()
      counter.start()

      val producer = new ProducerActor(counter)
      producer.start()

      producer.send(IncrementMessage())

      ScalazActorState.awaitTermination()
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double): Unit = {
      if (lastIteration) {
        ScalazPool.shutdown()
      }
    }
  }

  private case class IncrementMessage()

  private case class RetrieveMessage(sender: ScalazActor[AnyRef])

  private case class ResultMessage(result: Int)

  private class ProducerActor(counter: ScalazActor[AnyRef]) extends ScalazActor[AnyRef] {

    private val self = this

    override def process(msg: AnyRef) {
      msg match {
        case m: IncrementMessage =>

          var i = 0
          while (i < CountingConfig.N) {
            counter.send(m)
            i += 1
          }

          counter.send(RetrieveMessage(self))

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

  private class CountingActor extends ScalazActor[AnyRef] {

    private var count = 0

    override def process(msg: AnyRef) {
      msg match {
        case m: IncrementMessage =>
          count += 1
        case m: RetrieveMessage =>
          m.sender.send(ResultMessage(count))
          exit()
      }
    }
  }

}
