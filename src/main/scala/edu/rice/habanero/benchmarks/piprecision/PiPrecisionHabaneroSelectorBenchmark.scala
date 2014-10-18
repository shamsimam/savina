package edu.rice.habanero.benchmarks.piprecision

import java.math.BigDecimal
import java.util.concurrent.atomic.AtomicInteger

import edu.rice.habanero.actors.HabaneroSelector
import edu.rice.habanero.benchmarks.piprecision.PiPrecisionConfig.{StartMessage, StopMessage}
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}
import edu.rice.hj.Module0._
import edu.rice.hj.api.HjSuspendable

/**
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object PiPrecisionHabaneroSelectorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new PiPrecisionHabaneroSelectorBenchmark)
  }

  private final class PiPrecisionHabaneroSelectorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      PiPrecisionConfig.parseArgs(args)
    }

    def printArgInfo() {
      PiPrecisionConfig.printArgs()
    }

    def runIteration() {
      val numWorkers: Int = PiPrecisionConfig.NUM_WORKERS
      val precision: Int = PiPrecisionConfig.PRECISION
      finish(new HjSuspendable {
        override def run() = {
          val master = new Master(numWorkers, precision)
          master.start()
          master.send(0, StartMessage.ONLY)
        }
      })
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
    }
  }

  private class Master(numWorkers: Int, scale: Int) extends HabaneroSelector[AnyRef](1) {

    private final val workers = Array.tabulate[Worker](numWorkers)(i => new Worker(this, i))
    private var result: BigDecimal = BigDecimal.ZERO
    private final val tolerance = BigDecimal.ONE.movePointLeft(scale)
    private final val numWorkersTerminated: AtomicInteger = new AtomicInteger(0)
    private var numTermsRequested: Int = 0
    private var numTermsReceived: Int = 0
    private var stopRequests: Boolean = false

    override def onPostStart() {
      workers.foreach(loopWorker => {
        loopWorker.start()
      })
    }

    /**
     * Generates work for the given worker
     *
     * @param workerId the id of te worker to send work
     */
    private def generateWork(workerId: Int) {
      val wm: PiPrecisionConfig.WorkMessage = new PiPrecisionConfig.WorkMessage(scale, numTermsRequested)
      workers(workerId).send(0, wm)
      numTermsRequested += 1
    }

    def requestWorkersToExit() {
      workers.foreach(loopWorker => {
        loopWorker.send(0, StopMessage.ONLY)
      })
    }

    override def process(msg: AnyRef) {
      msg match {
        case rm: PiPrecisionConfig.ResultMessage =>
          numTermsReceived += 1
          result = result.add(rm.result)
          if (rm.result.compareTo(tolerance) <= 0) {
            stopRequests = true
          }
          if (!stopRequests) {
            generateWork(rm.workerId)
          }
          if (numTermsReceived == numTermsRequested) {
            requestWorkersToExit()
          }
        case _: PiPrecisionConfig.StopMessage =>
          val numTerminated: Int = numWorkersTerminated.incrementAndGet
          if (numTerminated == numWorkers) {
            exit()
          }
        case _: PiPrecisionConfig.StartMessage =>
          var t: Int = 0
          while (t < Math.min(scale, 10 * numWorkers)) {
            generateWork(t % numWorkers)
            t += 1
          }
        case message =>
          val ex = new IllegalArgumentException("Unsupported message: " + message)
          ex.printStackTrace(System.err)
      }
    }

    def getResult: String = {
      result.toPlainString
    }
  }

  private class Worker(master: Master, id: Int) extends HabaneroSelector[AnyRef](1) {

    override def process(msg: AnyRef) {
      msg match {
        case _: PiPrecisionConfig.StopMessage =>
          master.send(0, new PiPrecisionConfig.StopMessage)
          exit()
        case wm: PiPrecisionConfig.WorkMessage =>
          val result: BigDecimal = PiPrecisionConfig.calculateBbpTerm(wm.scale, wm.term)
          master.send(0, new PiPrecisionConfig.ResultMessage(result, id))
        case message =>
          val ex = new IllegalArgumentException("Unsupported message: " + message)
          ex.printStackTrace(System.err)
      }
    }
  }

}
