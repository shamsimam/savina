package edu.rice.habanero.benchmarks.pingpong

import edu.rice.habanero.actors.HabaneroSelector
import edu.rice.habanero.benchmarks.pingpong.PingPongConfig.{Message, PingMessage, StartMessage, StopMessage}
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}
import edu.rice.hj.Module0._
import edu.rice.hj.api.HjSuspendable

/**
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object PingPongHabaneroSelectorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new PingPongHabaneroSelectorBenchmark)
  }

  private final class PingPongHabaneroSelectorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      PingPongConfig.parseArgs(args)
    }

    def printArgInfo() {
      PingPongConfig.printArgs()
    }

    def runIteration() {
      finish(new HjSuspendable {
        override def run() = {
          val pong = new PongSelector()
          val ping = new PingSelector(PingPongConfig.N, pong)
          ping.start()
          pong.start()
          ping.send(0, StartMessage.ONLY)
        }
      })
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
    }
  }

  private class PingSelector(count: Int, pong: HabaneroSelector[PingPongConfig.Message]) extends HabaneroSelector[Message](1) {

    private var pingsLeft: Int = count

    override def process(msg: PingPongConfig.Message) {
      msg match {
        case _: PingPongConfig.StartMessage =>
          pong.send(0, new PingPongConfig.SendPingMessage(this))
          pingsLeft = pingsLeft - 1
        case _: PingPongConfig.PingMessage =>
          pong.send(0, new PingPongConfig.SendPingMessage(this))
          pingsLeft = pingsLeft - 1
        case _: PingPongConfig.SendPongMessage =>
          if (pingsLeft > 0) {
            this.send(0, PingMessage.ONLY)
          } else {
            pong.send(0, StopMessage.ONLY)
            exit()
          }
        case message =>
          val ex = new IllegalArgumentException("Unsupported message: " + message)
          ex.printStackTrace(System.err)
      }
    }
  }

  private class PongSelector extends HabaneroSelector[Message](1) {
    private var pongCount: Int = 0

    override def process(msg: PingPongConfig.Message) {
      msg match {
        case message: PingPongConfig.SendPingMessage =>
          val sender = message.sender.asInstanceOf[HabaneroSelector[PingPongConfig.Message]]
          sender.send(0, new PingPongConfig.SendPongMessage(this))
          pongCount = pongCount + 1
        case _: PingPongConfig.StopMessage =>
          exit()
        case message =>
          val ex = new IllegalArgumentException("Unsupported message: " + message)
          ex.printStackTrace(System.err)
      }
    }
  }

}
