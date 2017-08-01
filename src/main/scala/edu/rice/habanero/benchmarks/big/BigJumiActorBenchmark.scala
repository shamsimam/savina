package edu.rice.habanero.benchmarks.big

import edu.rice.habanero.actors.{JumiActor, JumiActorState, JumiPool}
import edu.rice.habanero.benchmarks.big.BigConfig.{ExitMessage, Message, PingMessage, PongMessage}
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner, PseudoRandom}

/**
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object BigJumiActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new BigJumiActorBenchmark)
  }

  private final class BigJumiActorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      BigConfig.parseArgs(args)
    }

    def printArgInfo() {
      BigConfig.printArgs()
    }

    def runIteration() {

      val sinkActor = new SinkActor(BigConfig.W)
      sinkActor.start()

      val bigActors = Array.tabulate[JumiActor[AnyRef]](BigConfig.W)(i => {
        val loopActor = new BigActor(i, BigConfig.N, sinkActor)
        loopActor.start()
        loopActor
      })

      val neighborMessage = new NeighborMessage(bigActors)
      sinkActor.send(neighborMessage)
      bigActors.foreach(loopActor => {
        loopActor.send(neighborMessage)
      })

      bigActors.foreach(loopActor => {
        loopActor.send(new PongMessage(-1))
      })

      JumiActorState.awaitTermination()
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double): Unit = {
      if (lastIteration) {
        JumiPool.shutdown()
      }
    }
  }

  private case class NeighborMessage(neighbors: Array[JumiActor[AnyRef]]) extends Message

  private class BigActor(id: Int, numMessages: Int, sinkActor: JumiActor[AnyRef]) extends JumiActor[AnyRef] {

    private var numPings = 0
    private var expPinger = -1
    private val random = new PseudoRandom(id)
    private var neighbors: Array[JumiActor[AnyRef]] = null

    private val myPingMessage = new PingMessage(id)
    private val myPongMessage = new PongMessage(id)

    override def process(msg: AnyRef) {
      msg match {
        case pm: PingMessage =>

          val sender = neighbors(pm.sender)
          sender.send(myPongMessage)

        case pm: PongMessage =>

          if (pm.sender != expPinger) {
            println("ERROR: Expected: " + expPinger + ", but received ping from " + pm.sender)
          }
          if (numPings == numMessages) {
            sinkActor.send(ExitMessage.ONLY)
          } else {
            sendPing()
            numPings += 1
          }

        case em: ExitMessage =>

          exit()

        case nm: NeighborMessage =>

          neighbors = nm.neighbors
      }
    }

    private def sendPing(): Unit = {
      val target = random.nextInt(neighbors.size)
      val targetActor = neighbors(target)

      expPinger = target
      targetActor.send(myPingMessage)
    }
  }

  private class SinkActor(numWorkers: Int) extends JumiActor[AnyRef] {

    private var numMessages = 0
    private var neighbors: Array[JumiActor[AnyRef]] = null

    override def process(msg: AnyRef) {
      msg match {
        case em: ExitMessage =>

          numMessages += 1
          if (numMessages == numWorkers) {
            neighbors.foreach(loopWorker => loopWorker.send(ExitMessage.ONLY))
            exit()
          }

        case nm: NeighborMessage =>

          neighbors = nm.neighbors
      }
    }
  }

}
