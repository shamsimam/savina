package edu.rice.habanero.benchmarks.threadring

import edu.rice.habanero.actors.{GparsActor, GparsActorState, GparsPool}
import edu.rice.habanero.benchmarks.threadring.ThreadRingConfig.{DataMessage, ExitMessage, PingMessage}
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}

/**
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object ThreadRingGparsActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new ThreadRingGparsActorBenchmark)
  }

  private final class ThreadRingGparsActorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      ThreadRingConfig.parseArgs(args)
    }

    def printArgInfo() {
      ThreadRingConfig.printArgs()
    }

    def runIteration() {

      val numActorsInRing = ThreadRingConfig.N
      val ringActors = Array.tabulate[GparsActor[AnyRef]](numActorsInRing)(i => {
        val loopActor = new ThreadRingActor(i, numActorsInRing)
        loopActor.start()
        loopActor
      })

      for ((loopActor, i) <- ringActors.view.zipWithIndex) {
        val nextActor = ringActors((i + 1) % numActorsInRing)
        loopActor.send(new DataMessage(nextActor))
      }

      ringActors(0).send(new PingMessage(ThreadRingConfig.R))

      GparsActorState.awaitTermination()
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double): Unit = {
      if (lastIteration) {
        GparsPool.shutdown()
      }
    }
  }

  private class ThreadRingActor(id: Int, numActorsInRing: Int) extends GparsActor[AnyRef] {

    private var nextActor: GparsActor[AnyRef] = null

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

          nextActor = dm.data.asInstanceOf[GparsActor[AnyRef]]
      }
    }
  }

}
