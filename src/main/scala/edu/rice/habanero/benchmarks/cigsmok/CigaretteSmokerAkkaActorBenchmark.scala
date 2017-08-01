package edu.rice.habanero.benchmarks.cigsmok

import akka.actor.{ActorRef, Props}
import edu.rice.habanero.actors.{AkkaActor, AkkaActorState}
import edu.rice.habanero.benchmarks.cigsmok.CigaretteSmokerConfig.{ExitMessage, StartMessage, StartSmoking, StartedSmoking}
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner, PseudoRandom}

/**
 * Based on solution in <a href="http://en.wikipedia.org/wiki/Cigarette_smokers_problem">Wikipedia</a> where resources are acquired instantaneously.
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object CigaretteSmokerAkkaActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new CigaretteSmokerAkkaActorBenchmark)
  }

  private final class CigaretteSmokerAkkaActorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      CigaretteSmokerConfig.parseArgs(args)
    }

    def printArgInfo() {
      CigaretteSmokerConfig.printArgs()
    }

    def runIteration() {

      val system = AkkaActorState.newActorSystem("CigaretteSmoker")

      val arbiterActor = system.actorOf(Props(new ArbiterActor(CigaretteSmokerConfig.R, CigaretteSmokerConfig.S)))
      AkkaActorState.startActor(arbiterActor)

      arbiterActor ! StartMessage.ONLY

      AkkaActorState.awaitTermination(system)
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
    }
  }

  private class ArbiterActor(numRounds: Int, numSmokers: Int) extends AkkaActor[AnyRef] {

    private val smokerActors = Array.tabulate[ActorRef](numSmokers)(i =>
      context.system.actorOf(Props(new SmokerActor(self))))
    private val random = new PseudoRandom(numRounds * numSmokers)
    private var roundsSoFar = 0

    override def onPostStart() {
      smokerActors.foreach(loopActor => {
        AkkaActorState.startActor(loopActor)
      })
    }

    override def process(msg: AnyRef) {
      msg match {
        case sm: StartMessage =>

          // choose a random smoker to start smoking
          notifyRandomSmoker()

        case sm: StartedSmoking =>

          // resources are off the table, can place new ones on the table
          roundsSoFar += 1
          if (roundsSoFar >= numRounds) {
            // had enough, now exit
            requestSmokersToExit()
            exit()
          } else {
            // choose a random smoker to start smoking
            notifyRandomSmoker()
          }
      }
    }

    private def notifyRandomSmoker() {
      // assume resources grabbed instantaneously
      val newSmokerIndex = Math.abs(random.nextInt()) % numSmokers
      val busyWaitPeriod = random.nextInt(1000) + 10
      smokerActors(newSmokerIndex) ! new StartSmoking(busyWaitPeriod)
    }

    private def requestSmokersToExit() {
      smokerActors.foreach(loopActor => {
        loopActor ! ExitMessage.ONLY
      })
    }
  }

  private class SmokerActor(arbiterActor: ActorRef) extends AkkaActor[AnyRef] {
    override def process(msg: AnyRef) {
      msg match {
        case sm: StartSmoking =>

          // notify arbiter that started smoking
          arbiterActor ! StartedSmoking.ONLY
          // now smoke cigarette
          CigaretteSmokerConfig.busyWait(sm.busyWaitPeriod)

        case em: ExitMessage =>

          exit()
      }
    }
  }

}
