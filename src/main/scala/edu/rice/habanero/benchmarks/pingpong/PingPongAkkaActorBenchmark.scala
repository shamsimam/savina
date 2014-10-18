package edu.rice.habanero.benchmarks.pingpong

import akka.actor.{ActorRef, Props}
import edu.rice.habanero.actors.{AkkaActor, AkkaActorState}
import edu.rice.habanero.benchmarks.pingpong.PingPongConfig.{Message, PingMessage, StartMessage, StopMessage}
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}

/**
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object PingPongAkkaActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new PingPongAkkaActorBenchmark)
  }

  private final class PingPongAkkaActorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      PingPongConfig.parseArgs(args)
    }

    def printArgInfo() {
      PingPongConfig.printArgs()
    }

    def runIteration() {

      val system = AkkaActorState.newActorSystem("PingPong")

      val pong = system.actorOf(Props(new PongActor()))
      val ping = system.actorOf(Props(new PingActor(PingPongConfig.N, pong)))

      AkkaActorState.startActor(ping)
      AkkaActorState.startActor(pong)

      ping ! StartMessage.ONLY

      AkkaActorState.awaitTermination(system)
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
    }
  }

  private class PingActor(count: Int, pong: ActorRef) extends AkkaActor[Message] {

    private var pingsLeft: Int = count

    override def process(msg: PingPongConfig.Message) {
      msg match {
        case _: PingPongConfig.StartMessage =>
          pong ! new PingPongConfig.SendPingMessage(self)
          pingsLeft = pingsLeft - 1
        case _: PingPongConfig.PingMessage =>
          pong ! new PingPongConfig.SendPingMessage(self)
          pingsLeft = pingsLeft - 1
        case _: PingPongConfig.SendPongMessage =>
          if (pingsLeft > 0) {
            self ! PingMessage.ONLY
          } else {
            pong ! StopMessage.ONLY
            exit()
          }
        case message =>
          val ex = new IllegalArgumentException("Unsupported message: " + message)
          ex.printStackTrace(System.err)
      }
    }
  }

  private class PongActor extends AkkaActor[Message] {
    private var pongCount: Int = 0

    override def process(msg: PingPongConfig.Message) {
      msg match {
        case message: PingPongConfig.SendPingMessage =>
          val sender = message.sender.asInstanceOf[ActorRef]
          sender ! new PingPongConfig.SendPongMessage(self)
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
