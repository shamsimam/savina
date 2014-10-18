package edu.rice.habanero.benchmarks.threadring

import edu.rice.habanero.actors.HabaneroActor
import edu.rice.habanero.benchmarks.threadring.ThreadRingConfig.{DataMessage, ExitMessage, PingMessage}
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}
import edu.rice.hj.Module0._
import edu.rice.hj.api.HjSuspendable

/**
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object ThreadRingHabaneroActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new ThreadRingHabaneroActorBenchmark)
  }

  private final class ThreadRingHabaneroActorBenchmark extends Benchmark {
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
          val ringActors = Array.tabulate[HabaneroActor[AnyRef]](numActorsInRing)(i => {
            val loopActor = new ThreadRingActor(i, numActorsInRing)
            loopActor.start()
            loopActor
          })

          for ((loopActor, i) <- ringActors.view.zipWithIndex) {
            val nextActor = ringActors((i + 1) % numActorsInRing)
            loopActor.send(new DataMessage(nextActor))
          }

          ringActors(0).send(new PingMessage(ThreadRingConfig.R))
        }
      })
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
    }
  }

  private class ThreadRingActor(id: Int, numActorsInRing: Int) extends HabaneroActor[AnyRef] {

    private var nextActor: HabaneroActor[AnyRef] = null

    override def process(msg: AnyRef) {

      msg match {

        case pm: PingMessage =>

          if (pm.hasNext) {
            nextActor.send(pm.next())
          } else {
            nextActor.send(new ExitMessage(numActorsInRing))
          }

        case em: ExitMessage =>

          if (em.hasNext) {
            nextActor.send(em.next())
          }
          exit()

        case dm: DataMessage =>

          nextActor = dm.data.asInstanceOf[HabaneroActor[AnyRef]]
      }
    }
  }

}
