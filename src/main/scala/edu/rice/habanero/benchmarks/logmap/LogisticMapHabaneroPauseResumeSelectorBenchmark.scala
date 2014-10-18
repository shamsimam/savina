package edu.rice.habanero.benchmarks.logmap

import edu.rice.habanero.actors.{HabaneroActor, HabaneroSelector}
import edu.rice.habanero.benchmarks.logmap.LogisticMapConfig._
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}
import edu.rice.hj.Module0._
import edu.rice.hj.api.{HjDataDrivenFuture, HjRunnable, HjSuspendable}

/**
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object LogisticMapHabaneroPauseResumeSelectorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new LogisticMapHabaneroPauseResumeSelectorBenchmark)
  }

  private final class LogisticMapHabaneroPauseResumeSelectorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      LogisticMapConfig.parseArgs(args)
    }

    def printArgInfo() {
      LogisticMapConfig.printArgs()
    }

    def runIteration() {

      var master: Master = null
      finish(new HjSuspendable {
        override def run() = {
          master = new Master()
          master.start()
          master.send(StartMessage.ONLY)
        }
      })
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
    }
  }

  private class Master extends HabaneroActor[AnyRef] {

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
              loopWorker.send(0, NextTermMessage.ONLY)
            })
            i += 1
          }

          // workers should stop after all items have been computed
          workers.foreach(loopWorker => {
            loopWorker.send(0, GetTermMessage.ONLY)
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
              loopWorker.send(0, StopMessage.ONLY)
            })
            exit()
          }

        case message =>

          val ex = new IllegalArgumentException("Unsupported message: " + message)
          ex.printStackTrace(System.err)
      }
    }
  }

  private class SeriesWorker(id: Int, master: Master, computer: RateComputer, startTerm: Double) extends HabaneroSelector[AnyRef](1, false) {

    private final val self = this
    private final val curTerm = Array.tabulate[Double](1)(i => startTerm)

    override def process(theMsg: AnyRef) {
      theMsg match {
        case computeMessage: NextTermMessage =>

          val sender = newDataDrivenFuture[ResultMessage]()
          val newMessage = new ComputeMessage(sender, curTerm(0))
          computer.send(newMessage)
          disable(0)

          asyncNbAwait(sender, new HjRunnable {
            override def run() = {
              val resultMessage = sender.get()
              curTerm(0) = resultMessage.term
              enable(0)
              tryProcessMessage(0)
            }
          })

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

  private class RateComputer(rate: Double) extends HabaneroActor[AnyRef] {

    override def process(theMsg: AnyRef) {
      theMsg match {
        case computeMessage: ComputeMessage =>

          val result = computeNextTerm(computeMessage.term, rate)
          val resultMessage = new ResultMessage(result)
          val sender = computeMessage.sender.asInstanceOf[HjDataDrivenFuture[ResultMessage]]
          sender.put(resultMessage)

        case _: StopMessage =>

          exit()

        case message =>

          val ex = new IllegalArgumentException("Unsupported message: " + message)
          ex.printStackTrace(System.err)
      }
    }
  }

}
