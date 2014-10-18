package edu.rice.habanero.benchmarks.sieve

import edu.rice.habanero.actors.{LiftActor, LiftActorState, LiftPool}
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}

/**
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object SieveLiftActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new SieveLiftActorBenchmark)
  }

  private final class SieveLiftActorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      SieveConfig.parseArgs(args)
    }

    def printArgInfo() {
      SieveConfig.printArgs()
    }

    def runIteration() {

      val producerActor = new NumberProducerActor(SieveConfig.N)
      producerActor.start()

      val filterActor = new PrimeFilterActor(1, 2, SieveConfig.M)
      filterActor.start()

      producerActor.send(filterActor)

      LiftActorState.awaitTermination()
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double): Unit = {
      if (lastIteration) {
        LiftPool.shutdown()
      }
    }
  }

  case class LongBox(value: Long)

  private class NumberProducerActor(limit: Long) extends LiftActor[AnyRef] {
    override def process(msg: AnyRef) {
      msg match {
        case filterActor: PrimeFilterActor =>
          var candidate: Long = 3
          while (candidate < limit) {
            filterActor.send(LongBox(candidate))
            candidate += 2
          }
          filterActor.send("EXIT")
          exit()
      }
    }
  }

  private class PrimeFilterActor(val id: Int, val myInitialPrime: Long, numMaxLocalPrimes: Int) extends LiftActor[AnyRef] {

    val self = this
    var nextFilterActor: PrimeFilterActor = null
    val localPrimes = new Array[Long](numMaxLocalPrimes)

    var availableLocalPrimes = 1
    localPrimes(0) = myInitialPrime

    private def handleNewPrime(newPrime: Long): Unit = {
      if (SieveConfig.debug)
        println("Found new prime number " + newPrime)
      if (availableLocalPrimes < numMaxLocalPrimes) {
        // Store locally if there is space
        localPrimes(availableLocalPrimes) = newPrime
        availableLocalPrimes += 1
      } else {
        // Create a new actor to store the new prime
        nextFilterActor = new PrimeFilterActor(id + 1, newPrime, numMaxLocalPrimes)
        nextFilterActor.start()
      }
    }

    override def process(msg: AnyRef) {
      try {
        msg match {
          case candidate: LongBox =>
            val locallyPrime = SieveConfig.isLocallyPrime(candidate.value, localPrimes, 0, availableLocalPrimes)
            if (locallyPrime) {
              if (nextFilterActor != null) {
                // Pass along the chain to detect for 'primeness'
                nextFilterActor.send(candidate)
              } else {
                // Found a new prime!
                handleNewPrime(candidate.value)
              }
            }
          case x: String =>
            if (nextFilterActor != null) {
              // Signal next actor for termination
              nextFilterActor.send(x)
            } else {
              val totalPrimes = ((id - 1) * numMaxLocalPrimes) + availableLocalPrimes
              println("Total primes = " + totalPrimes)
            }
            if (SieveConfig.debug)
              println("Terminating prime actor for number " + myInitialPrime)
            exit()
        }
      } catch {
        case e: Exception => e.printStackTrace()
      }
    }
  }

}
