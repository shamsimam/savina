package edu.rice.habanero.benchmarks.sor

import edu.rice.habanero.actors.{GparsActor, GparsActorState, GparsPool}
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}

import scala.collection.mutable.ListBuffer

/**
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object SucOverRelaxGparsActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new SucOverRelaxGparsActorBenchmark)
  }

  private final class SucOverRelaxGparsActorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      SucOverRelaxConfig.parseArgs(args)
    }

    def printArgInfo() {
      SucOverRelaxConfig.printArgs()
      SucOverRelaxConfig.initialize()
    }

    def runIteration() {
      val dataLevel = SucOverRelaxConfig.N
      val sorRunner = new SorRunner(dataLevel)
      sorRunner.start()
      sorRunner.send(SorBootMessage)

      GparsActorState.awaitTermination()
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double): Unit = {
      if (lastIteration) {
        GparsPool.shutdown()
      } else {
        SucOverRelaxConfig.initialize()
      }
    }
  }

  case class SorBorder(borderActors: Array[GparsActor[AnyRef]])

  case class SorBorderMessage(mBorder: SorBorder)

  case class SorStartMessage(mi: Int, mActors: Array[GparsActor[AnyRef]])

  case class SorValueMessage(v: Double)

  case object SorBootMessage

  case class SorResultMessage(mx: Int, my: Int, mv: Double, msgRcv: Int)

  private class SorRunner(n: Int) extends GparsActor[AnyRef] {

    private val s = SucOverRelaxConfig.DATA_SIZES(n)
    private val part = s / 2
    private val sorActors = Array.ofDim[GparsActor[AnyRef]](s * (part + 1))

    private def boot(): Unit = {

      val myBorder = Array.ofDim[GparsActor[AnyRef]](s)
      val randoms = SucOverRelaxConfig.A

      for (i <- 0 until s) {
        var c = i % 2
        for (j <- 0 until part) {
          val pos = i * (part + 1) + j
          c = 1 - c
          sorActors(pos) = new SorActor(pos, randoms(i)(j), c, s, part + 1, SucOverRelaxConfig.OMEGA, this, false)
          sorActors(pos).start()
          if (j == (part - 1)) {
            myBorder(i) = sorActors(pos)
          }
        }
      }

      val partialMatrix = Array.ofDim[Double](s, s - part)
      for (i <- 0 until s) {
        for (j <- 0 until s - part) {
          partialMatrix(i)(j) = randoms(i)(j + part)
        }
      }

      val sorPeer = new SorPeer(s, part, partialMatrix, SorBorder(myBorder), this)
      sorPeer.start()
      sorPeer.send(SorBootMessage)
    }

    private var gTotal = 0.0
    private var returned = 0
    private var totalMsgRcv = 0
    private var expectingBoot = true

    override def process(msg: AnyRef) {
      msg match {
        case SorBootMessage =>
          expectingBoot = false
          boot()
        case SorResultMessage(mx, my, mv, msgRcv) =>
          if (expectingBoot) {
            throw new IllegalStateException("SorRunner not booted yet!")
          }
          totalMsgRcv += msgRcv
          returned += 1
          gTotal += mv
          if (returned == (s * part) + 1) {
            SucOverRelaxConfig.jgfValidate(gTotal, n)
            exit()
          }
        case SorBorderMessage(mBorder) =>
          if (expectingBoot) {
            throw new IllegalStateException("SorRunner not booted yet!")
          }
          for (i <- 0 until s) {
            sorActors((i + 1) * (part + 1) - 1) = mBorder.borderActors(i)
          }
          for (i <- 0 until s) {
            for (j <- 0 until part) {
              val pos = i * (part + 1) + j
              sorActors(pos).send(SorStartMessage(SucOverRelaxConfig.JACOBI_NUM_ITER, sorActors))
            }
          }
      }
    }
  }

  class SorActor(
                  pos: Int,
                  var value: Double,
                  color: Int,
                  nx: Int,
                  ny: Int,
                  omega: Double,
                  sorSource: GparsActor[AnyRef],
                  peer: Boolean
                  ) extends GparsActor[AnyRef] {

    private val selfActor = this
    private final val x = pos / ny
    private final val y = pos % ny

    private final val omega_over_four = 0.25 * omega
    private final val one_minus_omega = 1.0 - omega

    private final val neighbors: Array[Int] =
      if (x > 0 && x < nx - 1 && y > 0 && y < ny - 1) {
        val tempNeighbors = Array.ofDim[Int](4)
        tempNeighbors(0) = calPos(x, y + 1)
        tempNeighbors(1) = calPos(x + 1, y)
        tempNeighbors(2) = calPos(x, y - 1)
        tempNeighbors(3) = calPos(x - 1, y)
        tempNeighbors
      } else if ((x == 0 || x == (nx - 1)) && (y == 0 || y == (ny - 1))) {
        val tempNeighbors = Array.ofDim[Int](2)
        tempNeighbors(0) = if (x == 0) calPos(x + 1, y) else calPos(x - 1, y)
        tempNeighbors(1) = if (y == 0) calPos(x, y + 1) else calPos(x, y - 1)
        tempNeighbors
      } else if ((x == 0 || x == (nx - 1)) || (y == 0 || y == (ny - 1))) {
        val tempNeighbors = Array.ofDim[Int](3)
        if (x == 0 || x == nx - 1) {
          tempNeighbors(0) = if (x == 0) calPos(x + 1, y) else calPos(x - 1, y)
          tempNeighbors(1) = calPos(x, y + 1)
          tempNeighbors(2) = calPos(x, y - 1)
        } else {
          tempNeighbors(0) = if (y == 0) calPos(x, y + 1) else calPos(x, y - 1)
          tempNeighbors(1) = calPos(x + 1, y)
          tempNeighbors(2) = calPos(x - 1, y)
        }
        tempNeighbors
      } else {
        Array.ofDim[Int](0)
      }

    private def calPos(x1: Int, y1: Int): Int = {
      x1 * ny + y1
    }

    private var iter = 0
    private var maxIter = 0
    private var msgRcv = 0
    private var sorActors: Array[GparsActor[AnyRef]] = null

    private var receivedVals = 0
    private var sum = 0.0
    private var expectingStart = true
    private val pendingMessages = new ListBuffer[AnyRef]()

    override def process(msg: AnyRef) {
      msg match {
        case SorStartMessage(mi, mActors) =>
          expectingStart = false
          sorActors = mActors
          maxIter = mi
          if (color == 1) {
            neighbors.foreach {
              loopNeighIndex =>
                sorActors(loopNeighIndex).send(SorValueMessage(value))
            }
            iter += 1
            msgRcv += 1
          }
          pendingMessages.foreach {
            loopMessage => selfActor.send(loopMessage)
          }
          pendingMessages.clear()

        case message: SorValueMessage =>
          if (expectingStart) {
            pendingMessages.append(message)
          } else {
            msgRcv += 1
            if (iter < maxIter) {
              receivedVals += 1
              sum += message.v
              if (receivedVals == neighbors.length) {
                value = (omega_over_four * sum) + (one_minus_omega * value)
                sum = 0.0
                receivedVals = 0

                neighbors.foreach {
                  loopNeighIndex =>
                    sorActors(loopNeighIndex).send(SorValueMessage(value))
                }
                iter += 1
              }
              if (iter == maxIter) {
                sorSource.send(SorResultMessage(x, y, value, msgRcv))
                exit()
              }
            }
          }
      }
    }
  }

  class SorPeer(
                 s: Int,
                 partStart: Int,
                 matrixPart: Array[Array[Double]],
                 border: SorBorder,
                 sorSource: SorRunner
                 ) extends GparsActor[AnyRef] {

    private val sorActors = Array.ofDim[GparsActor[AnyRef]](s * (s - partStart + 1))

    private def boot(): Unit = {
      val myBorder = Array.ofDim[GparsActor[AnyRef]](s)
      for (i <- 0 until s) {
        sorActors(i * (s - partStart + 1)) = border.borderActors(i)
      }
      for (i <- 0 until s) {
        var c = (i + partStart) % 2
        for (j <- 1 until (s - partStart + 1)) {
          val pos = i * (s - partStart + 1) + j
          c = 1 - c
          sorActors(pos) = new SorActor(pos, matrixPart(i)(j - 1), c, s, s - partStart + 1,
            SucOverRelaxConfig.OMEGA, this, true)
          sorActors(pos).start()

          if (j == 1) {
            myBorder(i) = sorActors(pos)
          }
        }
      }
      sorSource.send(SorBorderMessage(SorBorder(myBorder)))

      for (i <- 0 until s) {
        for (j <- 1 until (s - partStart + 1)) {
          val pos = i * (s - partStart + 1) + j
          sorActors(pos).send(SorStartMessage(SucOverRelaxConfig.JACOBI_NUM_ITER, sorActors))
        }
      }
    }

    private var gTotal = 0.0
    private var returned = 0
    private var totalMsgRcv = 0
    private var expectingBoot = true

    override def process(msg: AnyRef) {
      msg match {
        case SorBootMessage =>
          expectingBoot = false
          boot()
        case SorResultMessage(mx, my, mv, msgRcv) =>
          if (expectingBoot) {
            throw new IllegalStateException("SorPeer not booted yet!")
          }
          totalMsgRcv += msgRcv
          returned += 1
          gTotal += mv
          if (returned == s * (s - partStart)) {
            sorSource.send(SorResultMessage(-1, -1, gTotal, totalMsgRcv))
            exit()
          }
      }
    }
  }

}
