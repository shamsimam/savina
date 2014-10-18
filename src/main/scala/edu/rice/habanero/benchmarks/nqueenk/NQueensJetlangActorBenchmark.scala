package edu.rice.habanero.benchmarks.nqueenk

import edu.rice.habanero.actors.{JetlangActor, JetlangActorState, JetlangPool}
import edu.rice.habanero.benchmarks.nqueenk.NQueensConfig.{DoneMessage, ResultMessage, StopMessage}
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}

/**
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object NQueensJetlangActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new NQueensJetlangActorBenchmark)
  }

  private final class NQueensJetlangActorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      NQueensConfig.parseArgs(args)
    }

    def printArgInfo() {
      NQueensConfig.printArgs()
    }

    def runIteration() {
      val numWorkers: Int = NQueensConfig.NUM_WORKERS
      val priorities: Int = NQueensConfig.PRIORITIES
      val master: Array[Master] = Array(null)

      master(0) = new Master(numWorkers, priorities)
      master(0).start()

      JetlangActorState.awaitTermination()

      val expSolution = NQueensConfig.SOLUTIONS(NQueensConfig.SIZE - 1)
      val actSolution = master(0).resultCounter
      val solutionsLimit = NQueensConfig.SOLUTIONS_LIMIT
      val valid = actSolution >= solutionsLimit && actSolution <= expSolution

      printf(BenchmarkRunner.argOutputFormat, "Solutions found", actSolution)
      printf(BenchmarkRunner.argOutputFormat, "Result valid", valid)
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
      if (lastIteration) {
        JetlangPool.shutdown()
      }
    }
  }

  private class Master(numWorkers: Int, priorities: Int) extends JetlangActor[AnyRef] {

    private val solutionsLimit = NQueensConfig.SOLUTIONS_LIMIT
    private final val workers = new Array[Worker](numWorkers)
    var resultCounter: Long = 0
    private var messageCounter: Int = 0
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
      val inArray: Array[Int] = new Array[Int](0)
      val workMessage = new NQueensConfig.WorkMessage(priorities, inArray, 0)
      sendWork(workMessage)
    }

    private def sendWork(workMessage: NQueensConfig.WorkMessage) {
      workers(messageCounter).send(workMessage)
      messageCounter = (messageCounter + 1) % numWorkers
      numWorkSent += 1
    }

    override def process(theMsg: AnyRef) {
      theMsg match {
        case workMessage: NQueensConfig.WorkMessage =>
          sendWork(workMessage)
        case _: NQueensConfig.ResultMessage =>
          resultCounter += 1
          if (resultCounter == solutionsLimit) {
            requestWorkersToTerminate()
          }
        case _: NQueensConfig.DoneMessage =>
          numWorkCompleted += 1
          if (numWorkCompleted == numWorkSent) {
            requestWorkersToTerminate()
          }
        case _: NQueensConfig.StopMessage =>
          numWorkersTerminated += 1
          if (numWorkersTerminated == numWorkers) {
            exit()
          }
        case _ =>
      }
    }

    def requestWorkersToTerminate() {
      var i: Int = 0
      while (i < numWorkers) {
        workers(i).send(StopMessage.ONLY)
        i += 1
      }
    }
  }

  private class Worker(master: Master, id: Int) extends JetlangActor[AnyRef] {

    private final val threshold: Int = NQueensConfig.THRESHOLD
    private final val size: Int = NQueensConfig.SIZE

    override def process(theMsg: AnyRef) {
      theMsg match {
        case workMessage: NQueensConfig.WorkMessage =>
          nqueensKernelPar(workMessage)
          master.send(DoneMessage.ONLY)
        case _: NQueensConfig.StopMessage =>
          master.send(theMsg)
          exit()
        case _ =>
      }
    }

    private[nqueenk] def nqueensKernelPar(workMessage: NQueensConfig.WorkMessage) {
      val a: Array[Int] = workMessage.data
      val depth: Int = workMessage.depth
      if (size == depth) {
        master.send(ResultMessage.ONLY)
      } else if (depth >= threshold) {
        nqueensKernelSeq(a, depth)
      } else {
        val newPriority: Int = workMessage.priority - 1
        val newDepth: Int = depth + 1
        var i: Int = 0
        while (i < size) {
          val b: Array[Int] = new Array[Int](newDepth)
          System.arraycopy(a, 0, b, 0, depth)
          b(depth) = i
          if (NQueensConfig.boardValid(newDepth, b)) {
            master.send(new NQueensConfig.WorkMessage(newPriority, b, newDepth))
          }
          i += 1
        }
      }
    }

    private[nqueenk] def nqueensKernelSeq(a: Array[Int], depth: Int) {
      if (size == depth) {
        master.send(ResultMessage.ONLY)
      }
      else {
        val b: Array[Int] = new Array[Int](depth + 1)

        var i: Int = 0
        while (i < size) {
          System.arraycopy(a, 0, b, 0, depth)
          b(depth) = i
          if (NQueensConfig.boardValid(depth + 1, b)) {
            nqueensKernelSeq(b, depth + 1)
          }

          i += 1
        }
      }
    }
  }

}
