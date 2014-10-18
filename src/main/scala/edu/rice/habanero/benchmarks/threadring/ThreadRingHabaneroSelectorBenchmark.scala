package edu.rice.habanero.benchmarks.threadring

import edu.rice.habanero.actors.HabaneroSelector
import edu.rice.habanero.benchmarks.threadring.ThreadRingConfig.{DataMessage, ExitMessage, PingMessage}
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}
import edu.rice.hj.Module0._
import edu.rice.hj.api.HjSuspendable

/**
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object ThreadRingHabaneroSelectorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new ThreadRingHabaneroSelectorBenchmark)
  }

  private final class ThreadRingHabaneroSelectorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      ThreadRingConfig.parseArgs(args)
    }

    def printArgInfo() {
      ThreadRingConfig.printArgs()
    }

    def runIteration() {
      finish(new HjSuspendable {
        override def run() = {

          val numActorsInRing = ThreadRingConfig.N
          val ringActors = Array.tabulate[HabaneroSelector[AnyRef]](numActorsInRing)(i => {
            val loopActor = new ThreadRingActor(i, numActorsInRing)
            loopActor.start()
            loopActor
          })

          for ((loopActor, i) <- ringActors.view.zipWithIndex) {
            val nextActor = ringActors((i + 1) % numActorsInRing)
            loopActor.send(0, new DataMessage(nextActor))
          }

          ringActors(0).send(0, new PingMessage(ThreadRingConfig.R))
        }
      })
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
    }
  }

  private class ThreadRingActor(id: Int, numActorsInRing: Int) extends HabaneroSelector[AnyRef](1) {

    private var nextActor: HabaneroSelector[AnyRef] = null

    override def process(msg: AnyRef) {

      msg match {

        case pm: PingMessage =>

          if (pm.hasNext) {
            nextActor.send(0, pm.next())
          } else {
            nextActor.send(0, new ExitMessage(numActorsInRing))
          }

        case em: ExitMessage =>

          if (em.hasNext) {
            nextActor.send(0, em.next())
          }
          exit()

        case dm: DataMessage =>

          nextActor = dm.data.asInstanceOf[HabaneroSelector[AnyRef]]
      }
    }
  }

}
