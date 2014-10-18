package edu.rice.habanero.benchmarks.logmap

import edu.rice.habanero.actors.{ScalaActor, ScalaActorState}
import edu.rice.habanero.benchmarks.logmap.LogisticMapConfig._
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}

/**
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object LogisticMapScalaActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new LogisticMapScalaActorBenchmark)
  }

  private final class LogisticMapScalaActorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      LogisticMapConfig.parseArgs(args)
    }

    def printArgInfo() {
      LogisticMapConfig.printArgs()
    }

    def runIteration() {

      val master = new Master()
      master.start()
      master.send(StartMessage.ONLY)

      ScalaActorState.awaitTermination()
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
    }
  }

  private class Master extends ScalaActor[AnyRef] {

    private final val self = this

    private final val numComputers: Int = LogisticMapConfig.numSeries
    private final val computers = Array.tabulate[RateComputer](numComputers)(i => {
      val rate = LogisticMapConfig.startRate + (i * LogisticMapConfig.increment)
      new RateComputer(rate)
    })

    private final val numWorkers: Int = LogisticMapConfig.numSeries
    private final val workers = Array.tabulate[SeriesWorker](numWorkers)(i => {
      val rateComputer = computers(i % numComputers)
      val startTerm = i * LogisticMapConfig.increment
      new SeriesWorker(i, self, rateComputer, startTerm)
    })

    private var numWorkRequested: Int = 0
    private var numWorkReceived: Int = 0
    private var termsSum: Double = 0

    protected override def onPostStart() {
      computers.foreach(loopComputer => {
        loopComputer.start()
      })
      workers.foreach(loopWorker => {
        loopWorker.start()
      })
    }

    override def process(theMsg: AnyRef) {
      theMsg match {
        case _: StartMessage =>

          var i: Int = 0
          while (i < LogisticMapConfig.numTerms) {
            // request each worker to compute the next term
            workers.foreach(loopWorker => {
              loopWorker.send(NextTermMessage.ONLY)
            })
            i += 1
          }

          // workers should stop after all items have been computed
          workers.foreach(loopWorker => {
            loopWorker.send(GetTermMessage.ONLY)
            numWorkRequested += 1
          })

        case rm: ResultMessage =>

          termsSum += rm.term
          numWorkReceived += 1

          if (numWorkRequested == numWorkReceived) {

            println("Terms sum: " + termsSum)

            computers.foreach(loopComputer => {
              loopComputer.send(StopMessage.ONLY)
            })
            workers.foreach(loopWorker => {
              loopWorker.send(StopMessage.ONLY)
            })
            exit()
          }

        case message =>

          val ex = new IllegalArgumentException("Unsupported message: " + message)
          ex.printStackTrace(System.err)
      }
    }
  }

  private class SeriesWorker(id: Int, master: Master, computer: RateComputer, startTerm: Double) extends ScalaActor[AnyRef] {

    private final val self = this
    private final val curTerm = Array.tabulate[Double](1)(i => startTerm)

    override def process(theMsg: AnyRef) {
      theMsg match {
        case computeMessage: NextTermMessage =>

          val sender = self
          val newMessage = new ComputeMessage(sender, curTerm(0))
          computer.send(newMessage)

          receive {
            case resultMessage: ResultMessage =>
              curTerm(0) = resultMessage.term
          }

        case _: GetTermMessage =>

          master.send(new ResultMessage(curTerm(0)))

        case _: StopMessage =>

          exit()

        case message =>

          val ex = new IllegalArgumentException("Unsupported message: " + message)
          ex.printStackTrace(System.err)
      }
    }
  }

  private class RateComputer(rate: Double) extends ScalaActor[AnyRef] {

    override def process(theMsg: AnyRef) {
      theMsg match {
        case computeMessage: ComputeMessage =>

          val result = computeNextTerm(computeMessage.term, rate)
          val resultMessage = new ResultMessage(result)
          val sender = computeMessage.sender.asInstanceOf[ScalaActor[AnyRef]]
          sender.send(resultMessage)

        case _: StopMessage =>

          exit()

        case message =>

          val ex = new IllegalArgumentException("Unsupported message: " + message)
          ex.printStackTrace(System.err)
      }
    }
  }

}
