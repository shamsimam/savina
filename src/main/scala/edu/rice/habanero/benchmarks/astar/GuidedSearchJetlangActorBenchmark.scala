package edu.rice.habanero.benchmarks.astar

import java.util

import edu.rice.habanero.actors.{JetlangActor, JetlangActorState, JetlangPool}
import edu.rice.habanero.benchmarks.astar.GuidedSearchConfig._
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}

/**
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object GuidedSearchJetlangActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new GuidedSearchJetlangActorBenchmark)
  }

  private final class GuidedSearchJetlangActorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      GuidedSearchConfig.parseArgs(args)
    }

    def printArgInfo() {
      GuidedSearchConfig.printArgs()
    }

    def runIteration() {

      val master = new Master()
      master.start()

      JetlangActorState.awaitTermination()

      val nodesProcessed = GuidedSearchConfig.nodesProcessed()
      track("Nodes Processed", nodesProcessed)
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
      val valid = GuidedSearchConfig.validate()
      printf(BenchmarkRunner.argOutputFormat, "Result valid", valid)
      GuidedSearchConfig.initializeData()

      if (lastIteration) {
        JetlangPool.shutdown()
      }
    }
  }

  private class Master extends JetlangActor[AnyRef] {

    private final val numWorkers = GuidedSearchConfig.NUM_WORKERS
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
      sendWork(new WorkMessage(originNode, targetNode))
    }

    private def sendWork(workMessage: WorkMessage) {
      val workerIndex: Int = numWorkSent % numWorkers
      numWorkSent += 1
      workers(workerIndex).send(workMessage)
    }

    override def process(theMsg: AnyRef) {
      theMsg match {
        case workMessage: WorkMessage =>
          sendWork(workMessage)
        case _: ReceivedMessage =>
          numWorkCompleted += 1
          if (numWorkCompleted == numWorkSent) {
            requestWorkersToStop()
          }
        case _: DoneMessage =>
          requestWorkersToStop()
        case _: StopMessage =>
          numWorkersTerminated += 1
          if (numWorkersTerminated == numWorkers) {
            exit()
          }
        case _ =>
      }
    }

    private def requestWorkersToStop() {
      var i: Int = 0
      while (i < numWorkers) {
        workers(i).send(StopMessage.ONLY)
        i += 1
      }
    }
  }

  private class Worker(master: Master, id: Int) extends JetlangActor[AnyRef] {

    private final val threshold = GuidedSearchConfig.THRESHOLD

    override def process(theMsg: AnyRef) {
      theMsg match {
        case workMessage: WorkMessage =>
          search(workMessage)
          master.send(ReceivedMessage.ONLY)
        case _: StopMessage =>
          master.send(theMsg)
          exit()
        case _ =>
      }
    }

    private def search(workMessage: WorkMessage) {

      val targetNode = workMessage.target
      val workQueue = new util.LinkedList[GridNode]
      workQueue.add(workMessage.node)

      var nodesProcessed: Int = 0
      while (!workQueue.isEmpty && nodesProcessed < threshold) {

        nodesProcessed += 1
        GuidedSearchConfig.busyWait()

        val loopNode = workQueue.poll
        val numNeighbors: Int = loopNode.numNeighbors

        var i: Int = 0
        while (i < numNeighbors) {
          val loopNeighbor = loopNode.neighbor(i)
          val success: Boolean = loopNeighbor.setParent(loopNode)
          if (success) {
            if (loopNeighbor eq targetNode) {
              master.send(DoneMessage.ONLY)
              return
            } else {
              workQueue.add(loopNeighbor)
            }
          }
          i += 1
        }
      }

      while (!workQueue.isEmpty) {
        val loopNode = workQueue.poll
        val newWorkMessage = new WorkMessage(loopNode, targetNode)
        master.send(newWorkMessage)
      }
    }
  }

}
