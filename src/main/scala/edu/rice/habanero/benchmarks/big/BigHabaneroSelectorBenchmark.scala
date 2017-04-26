package edu.rice.habanero.benchmarks.big

import edu.rice.habanero.actors.HabaneroSelector
import edu.rice.habanero.benchmarks.big.BigConfig.{ExitMessage, Message, PingMessage, PongMessage}
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner, PseudoRandom}
import edu.rice.hj.Module0._
import edu.rice.hj.api.HjSuspendable

/**
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object BigHabaneroSelectorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new BigHabaneroSelectorBenchmark)
  }

  private final class BigHabaneroSelectorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      BigConfig.parseArgs(args)
    }

    def printArgInfo() {
      BigConfig.printArgs()
    }

    def runIteration() {
      finish(new HjSuspendable {
        override def run() = {

          val sinkActor = new SinkActor(BigConfig.W)
          sinkActor.start()

          val bigActors = Array.tabulate[HabaneroSelector[AnyRef]](BigConfig.W)(i => {
            val loopActor = new BigActor(i, BigConfig.N, sinkActor)
            loopActor.start()
            loopActor
          })

          val neighborMessage = new NeighborMessage(bigActors)
          sinkActor.send(0, neighborMessage)
          bigActors.foreach(loopActor => {
            loopActor.send(0, neighborMessage)
          })

          bigActors.foreach(loopActor => {
            loopActor.send(0, new PongMessage(-1))
          })

        }
      })
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
    }
  }

  private case class NeighborMessage(neighbors: Array[HabaneroSelector[AnyRef]]) extends Message

  private class BigActor(id: Int, numMessages: Int, sinkActor: HabaneroSelector[AnyRef]) extends HabaneroSelector[AnyRef](1) {

    private var numPings = 0
    private var expPinger = -1
    private val random = new PseudoRandom(id)
    private var neighbors: Array[HabaneroSelector[AnyRef]] = null

    private val myPingMessage = new PingMessage(id)
    private val myPongMessage = new PongMessage(id)

    override def process(msg: AnyRef) {
      msg match {
        case pm: PingMessage =>

          val sender = neighbors(pm.sender)
          sender.send(0, myPongMessage)

        case pm: PongMessage =>

          if (pm.sender != expPinger) {
            println("ERROR: Expected: " + expPinger + ", but received ping from " + pm.sender)
          }
          if (numPings == numMessages) {
            sinkActor.send(0, ExitMessage.ONLY)
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
      targetActor.send(0, myPingMessage)
    }
  }

  private class SinkActor(numWorkers: Int) extends HabaneroSelector[AnyRef](1) {

    private var numMessages = 0
    private var neighbors: Array[HabaneroSelector[AnyRef]] = null

    override def process(msg: AnyRef) {
      msg match {
        case em: ExitMessage =>

          numMessages += 1
          if (numMessages == numWorkers) {
            neighbors.foreach(loopWorker => loopWorker.send(0, ExitMessage.ONLY))
            exit()
          }

        case nm: NeighborMessage =>

          neighbors = nm.neighbors
      }
    }
  }

}
