package edu.rice.habanero.benchmarks.recmatmul

import edu.rice.habanero.actors.HabaneroActor
import edu.rice.habanero.benchmarks.recmatmul.MatMulConfig.{DoneMessage, StopMessage}
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}
import edu.rice.hj.Module0._
import edu.rice.hj.api.HjSuspendable

/**
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object MatMulHabaneroActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new MatMulHabaneroActorBenchmark)
  }

  private final class MatMulHabaneroActorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      MatMulConfig.parseArgs(args)
    }

    def printArgInfo() {
      MatMulConfig.printArgs()
    }

    def runIteration() {

      finish(new HjSuspendable {
        override def run() = {
          val master = new Master()
          master.start()
        }
      })
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
      val isValid = MatMulConfig.valid()
      printf(BenchmarkRunner.argOutputFormat, "Result valid", isValid)
      MatMulConfig.initializeData()
    }
  }

  private class Master extends HabaneroActor[AnyRef] {

    private final val numWorkers: Int = MatMulConfig.NUM_WORKERS
    private final val workers = new Array[Worker](numWorkers)
    private var numWorkersTerminated: Int = 0
    private var numWorkSent: Int = 0
    private var numWorkCompleted: Int = 0

    override def onPostStart() {

      var i: Int = 0
      while (i < numWorkers) {
        workers(i) = new Worker(this, i)
        workers(i).start()
        i += 1
      }

      val dataLength: Int = MatMulConfig.DATA_LENGTH
      val numBlocks: Int = MatMulConfig.DATA_LENGTH * MatMulConfig.DATA_LENGTH
      val workMessage: MatMulConfig.WorkMessage = new MatMulConfig.WorkMessage(0, 0, 0, 0, 0, 0, 0, numBlocks, dataLength)
      sendWork(workMessage)
    }

    private def sendWork(workMessage: MatMulConfig.WorkMessage) {
      val workerIndex: Int = (workMessage.srC + workMessage.scC) % numWorkers
      workers(workerIndex).send(workMessage)
      numWorkSent += 1
    }

    override def process(theMsg: AnyRef) {
      theMsg match {
        case workMessage: MatMulConfig.WorkMessage =>

          sendWork(workMessage)

        case _: MatMulConfig.DoneMessage =>

          numWorkCompleted += 1
          if (numWorkCompleted == numWorkSent) {
            var i: Int = 0
            while (i < numWorkers) {
              workers(i).send(StopMessage.ONLY)
              i += 1
            }
          }

        case _: MatMulConfig.StopMessage =>

          numWorkersTerminated += 1
          if (numWorkersTerminated == numWorkers) {
            exit()
          }

        case _ =>
          println("ERROR: Unexpected message: " + theMsg)
      }
    }
  }

  private class Worker(master: Master, id: Int) extends HabaneroActor[AnyRef] {

    private final val threshold: Int = MatMulConfig.BLOCK_THRESHOLD

    override def process(theMsg: AnyRef) {
      theMsg match {
        case workMessage: MatMulConfig.WorkMessage =>

          myRecMat(workMessage)
          master.send(DoneMessage.ONLY)

        case _: MatMulConfig.StopMessage =>

          master.send(theMsg)
          exit()

        case _ =>
          println("ERROR: Unexpected message: " + theMsg)
      }
    }

    private def myRecMat(workMessage: MatMulConfig.WorkMessage) {

      val srA: Int = workMessage.srA
      val scA: Int = workMessage.scA
      val srB: Int = workMessage.srB
      val scB: Int = workMessage.scB
      val srC: Int = workMessage.srC
      val scC: Int = workMessage.scC
      val numBlocks: Int = workMessage.numBlocks
      val dim: Int = workMessage.dim
      val newPriority: Int = workMessage.priority + 1
      if (numBlocks > threshold) {

        val zerDim: Int = 0
        val newDim: Int = dim / 2
        val newNumBlocks: Int = numBlocks / 4
        master.send(new MatMulConfig.WorkMessage(newPriority, srA + zerDim, scA + zerDim, srB + zerDim, scB + zerDim, srC + zerDim, scC + zerDim, newNumBlocks, newDim))
        master.send(new MatMulConfig.WorkMessage(newPriority, srA + zerDim, scA + newDim, srB + newDim, scB + zerDim, srC + zerDim, scC + zerDim, newNumBlocks, newDim))
        master.send(new MatMulConfig.WorkMessage(newPriority, srA + zerDim, scA + zerDim, srB + zerDim, scB + newDim, srC + zerDim, scC + newDim, newNumBlocks, newDim))
        master.send(new MatMulConfig.WorkMessage(newPriority, srA + zerDim, scA + newDim, srB + newDim, scB + newDim, srC + zerDim, scC + newDim, newNumBlocks, newDim))
        master.send(new MatMulConfig.WorkMessage(newPriority, srA + newDim, scA + zerDim, srB + zerDim, scB + zerDim, srC + newDim, scC + zerDim, newNumBlocks, newDim))
        master.send(new MatMulConfig.WorkMessage(newPriority, srA + newDim, scA + newDim, srB + newDim, scB + zerDim, srC + newDim, scC + zerDim, newNumBlocks, newDim))
        master.send(new MatMulConfig.WorkMessage(newPriority, srA + newDim, scA + zerDim, srB + zerDim, scB + newDim, srC + newDim, scC + newDim, newNumBlocks, newDim))
        master.send(new MatMulConfig.WorkMessage(newPriority, srA + newDim, scA + newDim, srB + newDim, scB + newDim, srC + newDim, scC + newDim, newNumBlocks, newDim))

      } else {

        val A: Array[Array[Double]] = MatMulConfig.A
        val B: Array[Array[Double]] = MatMulConfig.B
        val C: Array[Array[Double]] = MatMulConfig.C
        val endR: Int = srC + dim
        val endC: Int = scC + dim

        var i: Int = srC
        while (i < endR) {
          var j: Int = scC
          while (j < endC) {
            {
              var k: Int = 0
              while (k < dim) {
                C(i)(j) += A(i)(scA + k) * B(srB + k)(j)
                k += 1
              }
            }
            j += 1
          }
          i += 1
        }
      }
    }
  }

}
