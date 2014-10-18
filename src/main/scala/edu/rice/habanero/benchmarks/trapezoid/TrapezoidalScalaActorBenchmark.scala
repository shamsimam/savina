package edu.rice.habanero.benchmarks.trapezoid

import edu.rice.habanero.actors.{ScalaActor, ScalaActorState}
import edu.rice.habanero.benchmarks.trapezoid.TrapezoidalConfig.{ResultMessage, WorkMessage}
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}

/**
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object TrapezoidalScalaActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new TrapezoidalScalaActorBenchmark)
  }

  private final class TrapezoidalScalaActorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      TrapezoidalConfig.parseArgs(args)
    }

    def printArgInfo() {
      TrapezoidalConfig.printArgs()
    }

    def runIteration() {
      val numWorkers: Int = TrapezoidalConfig.W
      val precision: Double = (TrapezoidalConfig.R - TrapezoidalConfig.L) / TrapezoidalConfig.N

      val master = new Master(numWorkers)
      master.start()
      master.send(new WorkMessage(TrapezoidalConfig.L, TrapezoidalConfig.R, precision))

      ScalaActorState.awaitTermination()
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
    }
  }

  private class Master(numWorkers: Int) extends ScalaActor[AnyRef] {

    private final val workers = Array.tabulate[Worker](numWorkers)(i => new Worker(this, i))
    private var numTermsReceived: Int = 0
    private var resultArea: Double = 0.0

    override def onPostStart() {
      workers.foreach(loopWorker => {
        loopWorker.start()
      })
    }

    override def process(msg: AnyRef) {
      msg match {
        case rm: ResultMessage =>

          numTermsReceived += 1
          resultArea += rm.result

          if (numTermsReceived == numWorkers) {
            println("  Area: " + resultArea)
            exit()
          }

        case wm: WorkMessage =>

          val workerRange: Double = (wm.r - wm.l) / numWorkers
          for ((loopWorker, i) <- workers.view.zipWithIndex) {

            val wl = (workerRange * i) + wm.l
            val wr = wl + workerRange

            loopWorker.send(new WorkMessage(wl, wr, wm.h))
          }

        case message =>

          val ex = new IllegalArgumentException("Unsupported message: " + message)
          ex.printStackTrace(System.err)
      }
    }
  }

  private class Worker(master: Master, val id: Int) extends ScalaActor[AnyRef] {

    override def process(msg: AnyRef) {
      msg match {
        case wm: WorkMessage =>

          val n = ((wm.r - wm.l) / wm.h).asInstanceOf[Int]
          var accumArea = 0.0

          var i = 0
          while (i < n) {
            val lx = (i * wm.h) + wm.l
            val rx = lx + wm.h

            val ly = TrapezoidalConfig.fx(lx)
            val ry = TrapezoidalConfig.fx(rx)

            val area = 0.5 * (ly + ry) * wm.h
            accumArea += area

            i += 1
          }

          master.send(new ResultMessage(accumArea, id))
          exit()

        case message =>
          val ex = new IllegalArgumentException("Unsupported message: " + message)
          ex.printStackTrace(System.err)
      }
    }
  }

}
