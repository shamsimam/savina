package edu.rice.habanero.benchmarks.concdict

import java.util

import akka.actor.{ActorRef, Props}
import edu.rice.habanero.actors.{AkkaActor, AkkaActorState}
import edu.rice.habanero.benchmarks.concdict.DictionaryConfig.{DoWorkMessage, EndWorkMessage, ReadMessage, WriteMessage}
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}

/**
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object DictionaryAkkaActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new DictionaryAkkaActorBenchmark)
  }

  private final class DictionaryAkkaActorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      DictionaryConfig.parseArgs(args)
    }

    def printArgInfo() {
      DictionaryConfig.printArgs()
    }

    def runIteration() {
      val numWorkers: Int = DictionaryConfig.NUM_ENTITIES
      val numMessagesPerWorker: Int = DictionaryConfig.NUM_MSGS_PER_WORKER
      val system = AkkaActorState.newActorSystem("Dictionary")

      val master = system.actorOf(Props(new Master(numWorkers, numMessagesPerWorker)))
      AkkaActorState.startActor(master)

      AkkaActorState.awaitTermination(system)
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
    }
  }

  private class Master(numWorkers: Int, numMessagesPerWorker: Int) extends AkkaActor[AnyRef] {

    private final val workers = new Array[ActorRef](numWorkers)
    private final val dictionary = context.system.actorOf(Props(new Dictionary(DictionaryConfig.DATA_MAP)))
    private var numWorkersTerminated: Int = 0

    override def onPostStart() {
      AkkaActorState.startActor(dictionary)

      var i: Int = 0
      while (i < numWorkers) {
        workers(i) = context.system.actorOf(Props(new Worker(self, dictionary, i, numMessagesPerWorker)))
        AkkaActorState.startActor(workers(i))
        workers(i) ! DoWorkMessage.ONLY
        i += 1
      }
    }

    override def process(msg: AnyRef) {
      if (msg.isInstanceOf[DictionaryConfig.EndWorkMessage]) {
        numWorkersTerminated += 1
        if (numWorkersTerminated == numWorkers) {
          dictionary ! EndWorkMessage.ONLY
          exit()
        }
      }
    }
  }

  private class Worker(master: ActorRef, dictionary: ActorRef, id: Int, numMessagesPerWorker: Int) extends AkkaActor[AnyRef] {

    private final val writePercent = DictionaryConfig.WRITE_PERCENTAGE
    private var messageCount: Int = 0
    private final val random = new util.Random(id + numMessagesPerWorker + writePercent)

    override def process(msg: AnyRef) {
      messageCount += 1
      if (messageCount <= numMessagesPerWorker) {
        val anInt: Int = random.nextInt(100)
        if (anInt < writePercent) {
          dictionary ! new WriteMessage(self, random.nextInt, random.nextInt)
        } else {
          dictionary ! new ReadMessage(self, random.nextInt)
        }
      } else {
        master ! EndWorkMessage.ONLY
        exit()
      }
    }
  }

  private class Dictionary(initialState: util.Map[Integer, Integer]) extends AkkaActor[AnyRef] {

    private[concdict] final val dataMap = new util.HashMap[Integer, Integer](initialState)

    override def process(msg: AnyRef) {
      msg match {
        case writeMessage: DictionaryConfig.WriteMessage =>
          val key = writeMessage.key
          val value = writeMessage.value
          dataMap.put(key, value)
          val sender = writeMessage.sender.asInstanceOf[ActorRef]
          sender ! new DictionaryConfig.ResultMessage(self, value)
        case readMessage: DictionaryConfig.ReadMessage =>
          val value = dataMap.get(readMessage.key)
          val sender = readMessage.sender.asInstanceOf[ActorRef]
          sender ! new DictionaryConfig.ResultMessage(self, value)
        case _: DictionaryConfig.EndWorkMessage =>
          printf(BenchmarkRunner.argOutputFormat, "Dictionary Size", dataMap.size)
          exit()
        case _ =>
          System.err.println("Unsupported message: " + msg)
      }
    }
  }

}
