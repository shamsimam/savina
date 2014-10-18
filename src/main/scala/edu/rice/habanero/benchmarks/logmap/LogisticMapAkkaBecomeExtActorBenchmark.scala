package edu.rice.habanero.benchmarks.logmap

import akka.actor.{ActorRef, CustomStash, Props}
import edu.rice.habanero.actors.{AkkaActor, AkkaActorState}
import edu.rice.habanero.benchmarks.logmap.LogisticMapConfig._
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}

/**
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object LogisticMapAkkaBecomeExtActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new LogisticMapAkkaBecomeExtActorBenchmark)
  }

  private final class LogisticMapAkkaBecomeExtActorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      LogisticMapConfig.parseArgs(args)
    }

    def printArgInfo() {
      LogisticMapConfig.printArgs()
    }

    def runIteration() {

      val system = AkkaActorState.newActorSystem("LogisticMap")

      val master = system.actorOf(Props(new Master()))
      AkkaActorState.startActor(master)
      master ! StartMessage.ONLY

      AkkaActorState.awaitTermination(system)
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
    }
  }

  private class Master extends AkkaActor[AnyRef] {

    private final val numComputers: Int = LogisticMapConfig.numSeries
    private final val computers = Array.tabulate[ActorRef](numComputers)(i => {
      val rate = LogisticMapConfig.startRate + (i * LogisticMapConfig.increment)
      context.system.actorOf(Props(new RateComputer(rate)))
    })

    private final val numWorkers: Int = LogisticMapConfig.numSeries
    private final val workers = Array.tabulate[ActorRef](numWorkers)(i => {
      val rateComputer = computers(i % numComputers)
      val startTerm = i * LogisticMapConfig.increment
      context.system.actorOf(Props(new SeriesWorker(i, self, rateComputer, startTerm)))
    })

    private var numWorkRequested: Int = 0
    private var numWorkReceived: Int = 0
    private var termsSum: Double = 0

    protected override def onPostStart() {
      computers.foreach(loopComputer => {
        AkkaActorState.startActor(loopComputer)
      })
      workers.foreach(loopWorker => {
        AkkaActorState.startActor(loopWorker)
      })
    }

    override def process(theMsg: AnyRef) {
      theMsg match {
        case _: StartMessage =>

          var i: Int = 0
          while (i < LogisticMapConfig.numTerms) {
            // request each worker to compute the next term
            workers.foreach(loopWorker => {
              loopWorker ! NextTermMessage.ONLY
            })
            i += 1
          }

          // workers should stop after all items have been computed
          workers.foreach(loopWorker => {
            loopWorker ! GetTermMessage.ONLY
            numWorkRequested += 1
          })

        case rm: ResultMessage =>

          termsSum += rm.term
          numWorkReceived += 1

          if (numWorkRequested == numWorkReceived) {

            println("Terms sum: " + termsSum)

            computers.foreach(loopComputer => {
              loopComputer ! StopMessage.ONLY
            })
            workers.foreach(loopWorker => {
              loopWorker ! StopMessage.ONLY
            })
            exit()
          }

        case message =>

          val ex = new IllegalArgumentException("Unsupported message: " + message)
          ex.printStackTrace(System.err)
      }
    }
  }

  private class SeriesWorker(id: Int, master: ActorRef, computer: ActorRef, startTerm: Double) extends AkkaActor[AnyRef] with CustomStash {

    private final val curTerm = Array.tabulate[Double](1)(i => startTerm)

    override def process(theMsg: AnyRef) {
      theMsg match {
        case computeMessage: NextTermMessage =>

          val sender = self
          val newMessage = new ComputeMessage(sender, curTerm(0))
          computer ! newMessage

          context.become({
            case resultMessage: ResultMessage =>

              curTerm(0) = resultMessage.term

              unstash()
              context.unbecome()

            case message =>
              stash()

          }, discardOld = false)


        case _: GetTermMessage =>

          // do not reply to master if stash is not empty
          if (stashNonEmpty()) {
            stash()
          } else {
            master ! new ResultMessage(curTerm(0))
          }

        case _: StopMessage =>

          exit()

        case message =>

          val ex = new IllegalArgumentException("Unsupported message: " + message)
          ex.printStackTrace(System.err)
      }
    }
  }

  private class RateComputer(rate: Double) extends AkkaActor[AnyRef] {

    override def process(theMsg: AnyRef) {
      theMsg match {
        case computeMessage: ComputeMessage =>

          val result = computeNextTerm(computeMessage.term, rate)
          val resultMessage = new ResultMessage(result)
          sender() ! resultMessage

        case _: StopMessage =>

          exit()

        case message =>

          val ex = new IllegalArgumentException("Unsupported message: " + message)
          ex.printStackTrace(System.err)
      }
    }
  }

}
