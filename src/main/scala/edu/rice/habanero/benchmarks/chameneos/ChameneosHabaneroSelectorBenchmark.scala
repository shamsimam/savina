package edu.rice.habanero.benchmarks.chameneos

import edu.rice.habanero.actors.HabaneroSelector
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}
import edu.rice.hj.Module0._
import edu.rice.hj.api.HjSuspendable

/**
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object ChameneosHabaneroSelectorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new ChameneosHabaneroSelectorBenchmark)
  }

  private final class ChameneosHabaneroSelectorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      ChameneosConfig.parseArgs(args)
    }

    def printArgInfo() {
      ChameneosConfig.printArgs()
    }

    def runIteration() {
      finish(new HjSuspendable {
        override def run() = {
          val mallActor = new ChameneosMallActor(
            ChameneosConfig.numMeetings, ChameneosConfig.numChameneos)
          mallActor.start()
        }
      })
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
    }
  }

  private class ChameneosMallActor(var n: Int, numChameneos: Int)
    extends HabaneroSelector[ChameneosHelper.Message](ChameneosConfig.numMailboxes, true, ChameneosConfig.usePriorities) {

    private final val self: HabaneroSelector[ChameneosHelper.Message] = this
    private var waitingChameneo: HabaneroSelector[ChameneosHelper.Message] = null
    private var sumMeetings: Int = 0
    private var numFaded: Int = 0

    protected override def onPostStart() {
      startChameneos()
    }

    private def startChameneos() {
      val numMailboxes = ChameneosConfig.numMailboxes
      Array.tabulate[ChameneosChameneoActor](numChameneos)(i => {
        val color = ChameneosHelper.Color.values()(i % 3)
        val loopChamenos = new ChameneosChameneoActor(self, color, i, i % numMailboxes)
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
              val sender = message.sender.asInstanceOf[HabaneroSelector[ChameneosHelper.Message]]
              waitingChameneo = sender
            }
            else {
              n = n - 1
              waitingChameneo.send(0, msg)
              waitingChameneo = null
            }
          }
          else {
            val sender = message.sender.asInstanceOf[HabaneroSelector[ChameneosHelper.Message]]
            sender.send(0, new ChameneosHelper.ExitMsg(self))
          }
        case message =>
          val ex = new IllegalArgumentException("Unsupported message: " + message)
          ex.printStackTrace(System.err)
      }
    }
  }

  private class ChameneosChameneoActor(mall: HabaneroSelector[ChameneosHelper.Message], var color: ChameneosHelper.Color, id: Int, mallMailbox: Int)
    extends HabaneroSelector[ChameneosHelper.Message](1) {

    private var meetings: Int = 0

    protected override def onPostStart() {
      mall.send(0, new ChameneosHelper.MeetMsg(color, this))
    }

    override def process(msg: ChameneosHelper.Message) {
      msg match {
        case message: ChameneosHelper.MeetMsg =>
          val otherColor: ChameneosHelper.Color = message.color
          val sender = message.sender.asInstanceOf[HabaneroSelector[ChameneosHelper.Message]]
          color = ChameneosHelper.complement(color, otherColor)
          meetings = meetings + 1
          sender.send(0, new ChameneosHelper.ChangeMsg(color, this))
          mall.send(mallMailbox, new ChameneosHelper.MeetMsg(color, this))
        case message: ChameneosHelper.ChangeMsg =>
          color = message.color
          meetings = meetings + 1
          mall.send(mallMailbox, new ChameneosHelper.MeetMsg(color, this))
        case message: ChameneosHelper.ExitMsg =>
          val sender = message.sender.asInstanceOf[HabaneroSelector[ChameneosHelper.Message]]
          color = ChameneosHelper.fadedColor
          sender.send(mallMailbox, new ChameneosHelper.MeetingCountMsg(meetings, this))
          exit()
        case message =>
          val ex = new IllegalArgumentException("Unsupported message: " + message)
          ex.printStackTrace(System.err)
      }
    }
  }

}
