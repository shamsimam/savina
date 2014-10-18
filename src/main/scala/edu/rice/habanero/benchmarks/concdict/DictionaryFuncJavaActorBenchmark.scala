package edu.rice.habanero.benchmarks.concdict

import java.util

import edu.rice.habanero.actors.{FuncJavaActor, FuncJavaActorState, FuncJavaPool}
import edu.rice.habanero.benchmarks.concdict.DictionaryConfig.{DoWorkMessage, EndWorkMessage, ReadMessage, WriteMessage}
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}

/**
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object DictionaryFuncJavaActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new DictionaryFuncJavaActorBenchmark)
  }

  private final class DictionaryFuncJavaActorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      DictionaryConfig.parseArgs(args)
    }

    def printArgInfo() {
      DictionaryConfig.printArgs()
    }

    def runIteration() {
      val numWorkers: Int = DictionaryConfig.NUM_ENTITIES
      val numMessagesPerWorker: Int = DictionaryConfig.NUM_MSGS_PER_WORKER

      val master = new Master(numWorkers, numMessagesPerWorker)
      master.start()

      FuncJavaActorState.awaitTermination()
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
      if (lastIteration) {
        FuncJavaPool.shutdown()
      }
    }
  }

  private class Master(numWorkers: Int, numMessagesPerWorker: Int) extends FuncJavaActor[AnyRef] {

    private final val workers = new Array[FuncJavaActor[AnyRef]](numWorkers)
    private final val dictionary = new Dictionary(DictionaryConfig.DATA_MAP)
    private var numWorkersTerminated: Int = 0

    override def onPostStart() {
      dictionary.start()

      var i: Int = 0
      while (i < numWorkers) {
        workers(i) = new Worker(this, dictionary, i, numMessagesPerWorker)
        workers(i).start()
        workers(i).send(DoWorkMessage.ONLY)
        i += 1
      }
    }

    override def process(msg: AnyRef) {
      if (msg.isInstanceOf[DictionaryConfig.EndWorkMessage]) {
        numWorkersTerminated += 1
        if (numWorkersTerminated == numWorkers) {
          dictionary.send(EndWorkMessage.ONLY)
          exit()
        }
      }
    }
  }

  private class Worker(master: Master, dictionary: Dictionary, id: Int, numMessagesPerWorker: Int) extends FuncJavaActor[AnyRef] {

    private final val writePercent = DictionaryConfig.WRITE_PERCENTAGE
    private var messageCount: Int = 0
    private final val random = new util.Random(id + numMessagesPerWorker + writePercent)

    override def process(msg: AnyRef) {
      messageCount += 1
      if (messageCount <= numMessagesPerWorker) {
        val anInt: Int = random.nextInt(100)
        if (anInt < writePercent) {
          dictionary.send(new WriteMessage(this, random.nextInt, random.nextInt))
        } else {
          dictionary.send(new ReadMessage(this, random.nextInt))
        }
      } else {
        master.send(EndWorkMessage.ONLY)
        exit()
      }
    }
  }

  private class Dictionary(initialState: util.Map[Integer, Integer]) extends FuncJavaActor[AnyRef] {

    private[concdict] final val dataMap = new util.HashMap[Integer, Integer](initialState)

    override def process(msg: AnyRef) {
      msg match {
        case writeMessage: DictionaryConfig.WriteMessage =>
          val key = writeMessage.key
          val value = writeMessage.value
          dataMap.put(key, value)
          val sender = writeMessage.sender.asInstanceOf[FuncJavaActor[AnyRef]]
          sender.send(new DictionaryConfig.ResultMessage(this, value))
        case readMessage: DictionaryConfig.ReadMessage =>
          val value = dataMap.get(readMessage.key)
          val sender = readMessage.sender.asInstanceOf[FuncJavaActor[AnyRef]]
          sender.send(new DictionaryConfig.ResultMessage(this, value))
        case _: DictionaryConfig.EndWorkMessage =>
          printf(BenchmarkRunner.argOutputFormat, "Dictionary Size", dataMap.size)
          exit()
        case _ =>
          System.err.println("Unsupported message: " + msg)
      }
    }
  }

}
