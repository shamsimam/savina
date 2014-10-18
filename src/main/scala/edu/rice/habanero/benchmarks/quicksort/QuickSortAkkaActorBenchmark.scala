package edu.rice.habanero.benchmarks.quicksort

import java.util

import akka.actor.{ActorRef, Props}
import edu.rice.habanero.actors.{AkkaActor, AkkaActorState}
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}

/**
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object QuickSortAkkaActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new QuickSortAkkaActorBenchmark)
  }

  private final class QuickSortAkkaActorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      QuickSortConfig.parseArgs(args)
    }

    def printArgInfo() {
      QuickSortConfig.printArgs()
    }

    def runIteration() {

      val system = AkkaActorState.newActorSystem("QuickSort")

      val input = QuickSortConfig.randomlyInitArray()

      val rootActor = system.actorOf(Props(new QuickSortActor(null, PositionInitial)))
      AkkaActorState.startActor(rootActor)
      rootActor ! SortMessage(input)

      AkkaActorState.awaitTermination(system)
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
    }
  }

  private abstract class Position

  private case object PositionRight extends Position

  private case object PositionLeft extends Position

  private case object PositionInitial extends Position

  private abstract class Message

  private case class SortMessage(data: java.util.List[java.lang.Long]) extends Message

  private case class ResultMessage(data: java.util.List[java.lang.Long], position: Position) extends Message

  private class QuickSortActor(parent: ActorRef, positionRelativeToParent: Position) extends AkkaActor[AnyRef] {

    private var result: java.util.List[java.lang.Long] = null
    private var numFragments = 0

    def notifyParentAndTerminate() {

      if (positionRelativeToParent eq PositionInitial) {
        QuickSortConfig.checkSorted(result)
      }
      if (parent ne null) {
        parent ! ResultMessage(result, positionRelativeToParent)
      }
      exit()
    }

    override def process(msg: AnyRef) {
      msg match {
        case SortMessage(data) =>

          val dataLength: Int = data.size()
          if (dataLength < QuickSortConfig.T) {

            result = QuickSortConfig.quicksortSeq(data)
            notifyParentAndTerminate()

          } else {

            val dataLengthHalf = dataLength / 2
            val pivot = data.get(dataLengthHalf)

            val leftUnsorted = QuickSortConfig.filterLessThan(data, pivot)
            val leftActor = context.system.actorOf(Props(new QuickSortActor(self, PositionLeft)))
            AkkaActorState.startActor(leftActor)
            leftActor ! SortMessage(leftUnsorted)

            val rightUnsorted = QuickSortConfig.filterGreaterThan(data, pivot)
            val rightActor = context.system.actorOf(Props(new QuickSortActor(self, PositionRight)))
            AkkaActorState.startActor(rightActor)
            rightActor ! SortMessage(rightUnsorted)

            result = QuickSortConfig.filterEqualsTo(data, pivot)
            numFragments += 1
          }

        case ResultMessage(data, position) =>

          if (!data.isEmpty) {
            if (position eq PositionLeft) {
              val temp = new util.ArrayList[java.lang.Long]()
              temp.addAll(data)
              temp.addAll(result)
              result = temp
            } else if (position eq PositionRight) {
              val temp = new util.ArrayList[java.lang.Long]()
              temp.addAll(result)
              temp.addAll(data)
              result = temp
            }
          }

          numFragments += 1
          if (numFragments == 3) {
            notifyParentAndTerminate()
          }
      }
    }
  }

}
