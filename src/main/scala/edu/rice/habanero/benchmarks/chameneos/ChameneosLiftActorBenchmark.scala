package edu.rice.habanero.benchmarks.chameneos

import edu.rice.habanero.actors.{LiftActor, LiftActorState, LiftPool}
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}

/**
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object ChameneosLiftActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new ChameneosLiftActorBenchmark)
  }

  private final class ChameneosLiftActorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      ChameneosConfig.parseArgs(args)
    }

    def printArgInfo() {
      ChameneosConfig.printArgs()
    }

    def runIteration() {
      val mallActor = new ChameneosMallActor(
        ChameneosConfig.numMeetings, ChameneosConfig.numChameneos)
      mallActor.start()

      LiftActorState.awaitTermination()
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
      if (lastIteration) {
        LiftPool.shutdown()
      }
    }
  }

  private class ChameneosMallActor(var n: Int, numChameneos: Int) extends LiftActor[ChameneosHelper.Message] {

    private final val self: LiftActor[ChameneosHelper.Message] = this
    private var waitingChameneo: LiftActor[ChameneosHelper.Message] = null
    private var sumMeetings: Int = 0
    private var numFaded: Int = 0

    protected override def onPostStart() {
      startChameneos()
    }

    private def startChameneos() {
      Array.tabulate[ChameneosChameneoActor](numChameneos)(i => {
        val color = ChameneosHelper.Color.values()(i % 3)
        val loopChamenos = new ChameneosChameneoActor(self, color, i)
        loopChamenos.start()
        loopChamenos
      })
    }

    override def process(msg: ChameneosHelper.Message) {
      msg match {
        case message: ChameneosHelper.MeetingCountMsg =>
          numFaded = numFaded + 1
          sumMeetings = sumMeetings + message.count
          if (numFaded == numChameneos) {
            exit()
          }
        case message: ChameneosHelper.MeetMsg =>
          if (n > 0) {
            if (waitingChameneo == null) {
              val sender = message.sender.asInstanceOf[LiftActor[ChameneosHelper.Message]]
              waitingChameneo = sender
            }
            else {
              n = n - 1
              waitingChameneo.send(msg)
              waitingChameneo = null
            }
          }
          else {
            val sender = message.sender.asInstanceOf[LiftActor[ChameneosHelper.Message]]
            sender.send(new ChameneosHelper.ExitMsg(self))
          }
        case message =>
          val ex = new IllegalArgumentException("Unsupported message: " + message)
          ex.printStackTrace(System.err)
      }
    }
  }

  private class ChameneosChameneoActor(mall: LiftActor[ChameneosHelper.Message], var color: ChameneosHelper.Color, id: Int)
    extends LiftActor[ChameneosHelper.Message] {

    private var meetings: Int = 0

    protected override def onPostStart() {
      mall.send(new ChameneosHelper.MeetMsg(color, this))
    }

    override def process(msg: ChameneosHelper.Message) {
      msg match {
        case message: ChameneosHelper.MeetMsg =>
          val otherColor: ChameneosHelper.Color = message.color
          val sender = message.sender.asInstanceOf[LiftActor[ChameneosHelper.Message]]
          color = ChameneosHelper.complement(color, otherColor)
          meetings = meetings + 1
          sender.send(new ChameneosHelper.ChangeMsg(color, this))
          mall.send(new ChameneosHelper.MeetMsg(color, this))
        case message: ChameneosHelper.ChangeMsg =>
          color = message.color
          meetings = meetings + 1
          mall.send(new ChameneosHelper.MeetMsg(color, this))
        case message: ChameneosHelper.ExitMsg =>
          val sender = message.sender.asInstanceOf[LiftActor[ChameneosHelper.Message]]
          color = ChameneosHelper.fadedColor
          sender.send(new ChameneosHelper.MeetingCountMsg(meetings, this))
          exit()
        case message =>
          val ex = new IllegalArgumentException("Unsupported message: " + message)
          ex.printStackTrace(System.err)
      }
    }
  }

}
