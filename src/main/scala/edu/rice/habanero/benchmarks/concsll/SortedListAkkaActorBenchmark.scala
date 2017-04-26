package edu.rice.habanero.benchmarks.concsll

import akka.actor.{ActorRef, Props}
import edu.rice.habanero.actors.{AkkaActor, AkkaActorState}
import edu.rice.habanero.benchmarks.concsll.SortedListConfig.{DoWorkMessage, EndWorkMessage}
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner, PseudoRandom}

/**
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object SortedListAkkaActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new SortedListAkkaActorBenchmark)
  }

  private final class SortedListAkkaActorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      SortedListConfig.parseArgs(args)
    }

    def printArgInfo() {
      SortedListConfig.printArgs()
    }

    def runIteration() {
      val numWorkers: Int = SortedListConfig.NUM_ENTITIES
      val numMessagesPerWorker: Int = SortedListConfig.NUM_MSGS_PER_WORKER
      val system = AkkaActorState.newActorSystem("SortedList")

      val master = system.actorOf(Props(new Master(numWorkers, numMessagesPerWorker)))
      AkkaActorState.startActor(master)

      AkkaActorState.awaitTermination(system)
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
    }
  }

  private class Master(numWorkers: Int, numMessagesPerWorker: Int) extends AkkaActor[AnyRef] {

    private final val workers = new Array[ActorRef](numWorkers)
    private final val sortedList = context.system.actorOf(Props(new SortedList()))
    private var numWorkersTerminated: Int = 0

    override def onPostStart() {
      AkkaActorState.startActor(sortedList)

      var i: Int = 0
      while (i < numWorkers) {
        workers(i) = context.system.actorOf(Props(new Worker(self, sortedList, i, numMessagesPerWorker)))
        AkkaActorState.startActor(workers(i))
        workers(i) ! DoWorkMessage.ONLY
        i += 1
      }
    }

    override def process(msg: AnyRef) {
      if (msg.isInstanceOf[SortedListConfig.EndWorkMessage]) {
        numWorkersTerminated += 1
        if (numWorkersTerminated == numWorkers) {
          sortedList ! EndWorkMessage.ONLY
          exit()
        }
      }
    }
  }

  private class Worker(master: ActorRef, sortedList: ActorRef, id: Int, numMessagesPerWorker: Int) extends AkkaActor[AnyRef] {

    private final val writePercent = SortedListConfig.WRITE_PERCENTAGE
    private final val sizePercent = SortedListConfig.SIZE_PERCENTAGE
    private var messageCount: Int = 0
    private final val random = new PseudoRandom(id + numMessagesPerWorker + writePercent + sizePercent)

    override def process(msg: AnyRef) {
      messageCount += 1
      if (messageCount <= numMessagesPerWorker) {
        val anInt: Int = random.nextInt(100)
        if (anInt < sizePercent) {
          sortedList ! new SortedListConfig.SizeMessage(self)
        } else if (anInt < (sizePercent + writePercent)) {
          sortedList ! new SortedListConfig.WriteMessage(self, random.nextInt)
        } else {
          sortedList ! new SortedListConfig.ContainsMessage(self, random.nextInt)
        }
      } else {
        master ! EndWorkMessage.ONLY
        exit()
      }
    }
  }

  private class SortedList extends AkkaActor[AnyRef] {

    private[concsll] final val dataList = new SortedLinkedList[Integer]

    override def process(msg: AnyRef) {
      msg match {
        case writeMessage: SortedListConfig.WriteMessage =>
          val value: Int = writeMessage.value
          dataList.add(value)
          val sender = writeMessage.sender.asInstanceOf[ActorRef]
          sender ! new SortedListConfig.ResultMessage(self, value)
        case containsMessage: SortedListConfig.ContainsMessage =>
          val value: Int = containsMessage.value
          val result: Int = if (dataList.contains(value)) 1 else 0
          val sender = containsMessage.sender.asInstanceOf[ActorRef]
          sender ! new SortedListConfig.ResultMessage(self, result)
        case readMessage: SortedListConfig.SizeMessage =>
          val value: Int = dataList.size
          val sender = readMessage.sender.asInstanceOf[ActorRef]
          sender ! new SortedListConfig.ResultMessage(self, value)
        case _: SortedListConfig.EndWorkMessage =>
          printf(BenchmarkRunner.argOutputFormat, "List Size", dataList.size)
          exit()
        case _ =>
          System.err.println("Unsupported message: " + msg)
      }
    }
  }

}
