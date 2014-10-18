package edu.rice.habanero.benchmarks.sieve

import akka.actor.{ActorRef, Props}
import edu.rice.habanero.actors.{AkkaActor, AkkaActorState}
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}

/**
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object SieveAkkaActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new SieveAkkaActorBenchmark)
  }

  private final class SieveAkkaActorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      SieveConfig.parseArgs(args)
    }

    def printArgInfo() {
      SieveConfig.printArgs()
    }

    def runIteration() {

      val system = AkkaActorState.newActorSystem("Sieve")

      val producerActor = system.actorOf(Props(new NumberProducerActor(SieveConfig.N)))
      AkkaActorState.startActor(producerActor)

      val filterActor = system.actorOf(Props(new PrimeFilterActor(1, 2, SieveConfig.M)))
      AkkaActorState.startActor(filterActor)

      producerActor ! filterActor

      AkkaActorState.awaitTermination(system)
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
    }
  }

  case class LongBox(value: Long)

  private class NumberProducerActor(limit: Long) extends AkkaActor[AnyRef] {
    override def process(msg: AnyRef) {
      msg match {
        case filterActor: ActorRef =>
          var candidate: Long = 3
          while (candidate < limit) {
            filterActor ! LongBox(candidate)
            candidate += 2
          }
          filterActor ! "EXIT"
          exit()
      }
    }
  }

  private class PrimeFilterActor(val id: Int, val myInitialPrime: Long, numMaxLocalPrimes: Int) extends AkkaActor[AnyRef] {

    var nextFilterActor: ActorRef = null
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
        nextFilterActor = context.system.actorOf(Props(new PrimeFilterActor(id + 1, newPrime, numMaxLocalPrimes)))
        AkkaActorState.startActor(nextFilterActor)
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
                nextFilterActor ! candidate
              } else {
                // Found a new prime!
                handleNewPrime(candidate.value)
              }
            }
          case x: String =>
            if (nextFilterActor != null) {
              // Signal next actor for termination
              nextFilterActor ! x
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
