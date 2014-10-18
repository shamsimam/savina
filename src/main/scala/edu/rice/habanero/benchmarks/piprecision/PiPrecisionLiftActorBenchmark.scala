package edu.rice.habanero.benchmarks.piprecision

import java.math.BigDecimal
import java.util.concurrent.atomic.AtomicInteger

import edu.rice.habanero.actors.{LiftActor, LiftActorState, LiftPool}
import edu.rice.habanero.benchmarks.piprecision.PiPrecisionConfig.{StartMessage, StopMessage}
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}

/**
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object PiPrecisionLiftActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new PiPrecisionLiftActorBenchmark)
  }

  private final class PiPrecisionLiftActorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      PiPrecisionConfig.parseArgs(args)
    }

    def printArgInfo() {
      PiPrecisionConfig.printArgs()
    }

    def runIteration() {
      val numWorkers: Int = PiPrecisionConfig.NUM_WORKERS
      val precision: Int = PiPrecisionConfig.PRECISION

      val master = new Master(numWorkers, precision)
      master.start()
      master.send(StartMessage.ONLY)

      LiftActorState.awaitTermination()

    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
      if (lastIteration) {
        LiftPool.shutdown()
      }
    }
  }

  private class Master(numWorkers: Int, scale: Int) extends LiftActor[AnyRef] {

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
      workers(workerId).send(wm)
      numTermsRequested += 1
    }

    def requestWorkersToExit() {
      workers.foreach(loopWorker => {
        loopWorker.send(StopMessage.ONLY)
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

  private class Worker(master: Master, id: Int) extends LiftActor[AnyRef] {

    private var termsProcessed: Int = 0

    override def process(msg: AnyRef) {
      msg match {
        case _: PiPrecisionConfig.StopMessage =>
          master.send(new PiPrecisionConfig.StopMessage)
          exit()
        case wm: PiPrecisionConfig.WorkMessage =>
          termsProcessed += 1
          val result: BigDecimal = PiPrecisionConfig.calculateBbpTerm(wm.scale, wm.term)
          master.send(new PiPrecisionConfig.ResultMessage(result, id))
        case message =>
          val ex = new IllegalArgumentException("Unsupported message: " + message)
          ex.printStackTrace(System.err)
      }
    }
  }

}
