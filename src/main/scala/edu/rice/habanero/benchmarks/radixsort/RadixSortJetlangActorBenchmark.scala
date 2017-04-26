package edu.rice.habanero.benchmarks.radixsort

import edu.rice.habanero.actors.{JetlangActor, JetlangActorState, JetlangPool}
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner, PseudoRandom}

/**
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object RadixSortJetlangActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new RadixSortJetlangActorBenchmark)
  }

  private final class RadixSortJetlangActorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      RadixSortConfig.parseArgs(args)
    }

    def printArgInfo() {
      RadixSortConfig.printArgs()
    }

    def runIteration() {

      val validationActor = new ValidationActor(RadixSortConfig.N)
      validationActor.start()

      val sourceActor = new IntSourceActor(RadixSortConfig.N, RadixSortConfig.M, RadixSortConfig.S)
      sourceActor.start()

      var radix = RadixSortConfig.M / 2
      var nextActor: JetlangActor[AnyRef] = validationActor
      while (radix > 0) {

        val sortActor = new SortActor(RadixSortConfig.N, radix, nextActor)
        sortActor.start()

        radix /= 2
        nextActor = sortActor
      }

      sourceActor.send(NextActorMessage(nextActor))

      JetlangActorState.awaitTermination()
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double): Unit = {
      if (lastIteration) {
        JetlangPool.shutdown()
      }
    }
  }

  private case class NextActorMessage(actor: JetlangActor[AnyRef])

  private case class ValueMessage(value: Long)

  private class IntSourceActor(numValues: Int, maxValue: Long, seed: Long) extends JetlangActor[AnyRef] {

    val random = new PseudoRandom(seed)

    override def process(msg: AnyRef) {

      msg match {
        case nm: NextActorMessage =>

          var i = 0
          while (i < numValues) {

            val candidate = Math.abs(random.nextLong()) % maxValue
            val message = new ValueMessage(candidate)
            nm.actor.send(message)

            i += 1
          }

          exit()
      }
    }
  }

  private class SortActor(numValues: Int, radix: Long, nextActor: JetlangActor[AnyRef]) extends JetlangActor[AnyRef] {

    private val orderingArray = Array.ofDim[ValueMessage](numValues)
    private var valuesSoFar = 0
    private var j = 0

    override def process(msg: AnyRef): Unit = {
      msg match {
        case vm: ValueMessage =>

          valuesSoFar += 1

          val current = vm.value
          if ((current & radix) == 0) {
            nextActor.send(vm)
          } else {
            orderingArray(j) = vm
            j += 1
          }

          if (valuesSoFar == numValues) {

            var i = 0
            while (i < j) {
              nextActor.send(orderingArray(i))
              i += 1
            }

            exit()
          }
      }
    }
  }

  private class ValidationActor(numValues: Int) extends JetlangActor[AnyRef] {

    private var sumSoFar = 0.0
    private var valuesSoFar = 0
    private var prevValue = 0L
    private var errorValue = (-1L, -1)

    override def process(msg: AnyRef) {

      msg match {
        case vm: ValueMessage =>

          valuesSoFar += 1

          if (vm.value < prevValue && errorValue._1 < 0) {
            errorValue = (vm.value, valuesSoFar - 1)
          }
          prevValue = vm.value
          sumSoFar += prevValue

          if (valuesSoFar == numValues) {
            if (errorValue._1 >= 0) {
              println("ERROR: Value out of place: " + errorValue._1 + " at index " + errorValue._2)
            } else {
              println("Elements sum: " + sumSoFar)
            }
            exit()
          }
      }
    }
  }

}
