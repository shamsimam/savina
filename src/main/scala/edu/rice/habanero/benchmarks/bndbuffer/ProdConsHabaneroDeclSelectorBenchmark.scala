package edu.rice.habanero.benchmarks.bndbuffer

import edu.rice.habanero.actors.{HabaneroActor, HabaneroDeclarativeSelector}
import edu.rice.habanero.benchmarks.bndbuffer.ProdConsBoundedBufferConfig._
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}
import edu.rice.hj.Module0.finish
import edu.rice.hj.api.HjSuspendable

import scala.collection.mutable.ListBuffer

/**
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object ProdConsHabaneroDeclSelectorBenchmark {
  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new ProdConsHabaneroDeclSelectorBenchmark)
  }

  private final class ProdConsHabaneroDeclSelectorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      ProdConsBoundedBufferConfig.parseArgs(args)
    }

    def printArgInfo() {
      ProdConsBoundedBufferConfig.printArgs()
    }

    def runIteration() {

      finish(new HjSuspendable {
        override def run() = {
          val manager = new ManagerActor(
            ProdConsBoundedBufferConfig.bufferSize,
            ProdConsBoundedBufferConfig.numProducers,
            ProdConsBoundedBufferConfig.numConsumers,
            ProdConsBoundedBufferConfig.numItemsPerProducer)

          manager.start()
        }
      })
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
    }

    private class ManagerActor(bufferSize: Int, numProducers: Int, numConsumers: Int, numItemsPerProducer: Int)
      extends HabaneroDeclarativeSelector[AnyRef](MessageSource.values().length) {

      private val adjustedBufferSize: Int = bufferSize - numProducers
      private val pendingData = new ListBuffer[ProdConsBoundedBufferConfig.DataItemMessage]
      private var numTerminatedProducers: Int = 0

      private val producers = Array.tabulate[ProducerActor](numProducers)(i =>
        new ProducerActor(i, this, numItemsPerProducer))
      private val consumers = Array.tabulate[ConsumerActor](numConsumers)(i =>
        new ConsumerActor(i, this))

      // disable processing of request from consumers
      disable(MessageSource.CONSUMER)

      override def onPostStart() {
        consumers.foreach(loopConsumer => {
          loopConsumer.start()
        })

        producers.foreach(loopProducer => {
          loopProducer.start()
          loopProducer.send(ProduceDataMessage.ONLY)
        })
      }

      override def onPreExit() {
        consumers.foreach(loopConsumer => {
          loopConsumer.send(ConsumerExitMessage.ONLY)
        })
      }

      override def registerGuards(): Unit = {
        guard(MessageSource.PRODUCER, (msg: AnyRef) => pendingData.size < adjustedBufferSize)
        guard(MessageSource.CONSUMER, (msg: AnyRef) => pendingData.nonEmpty)
      }

      override def doProcess(theMsg: AnyRef) {
        theMsg match {
          case dm: ProdConsBoundedBufferConfig.DataItemMessage =>
            val producer: ProducerActor = dm.producer.asInstanceOf[ProducerActor]
            pendingData.append(dm)
            producer.send(ProduceDataMessage.ONLY)
          case cm: ProdConsBoundedBufferConfig.ConsumerAvailableMessage =>
            val consumer: ConsumerActor = cm.consumer.asInstanceOf[ConsumerActor]
            consumer.send(pendingData.remove(0))
            tryExit()
          case _: ProdConsBoundedBufferConfig.ProducerExitMessage =>
            numTerminatedProducers += 1
            tryExit()
          case msg =>
            val ex = new IllegalArgumentException("Unsupported message: " + msg)
            ex.printStackTrace(System.err)
        }
      }

      private def tryExit() {
        if (numTerminatedProducers == numProducers && pendingData.isEmpty) {
          exit()
        }
      }
    }

    private class ProducerActor(id: Int, manager: ManagerActor, numItemsToProduce: Int) extends HabaneroActor[AnyRef] {

      private var prodItem: Double = 0.0
      private var itemsProduced: Int = 0

      private def produceData() {
        prodItem = processItem(prodItem, prodCost)
        manager.send(MessageSource.PRODUCER, new ProdConsBoundedBufferConfig.DataItemMessage(prodItem, this))
        itemsProduced += 1
      }

      override def process(theMsg: AnyRef) {
        if (theMsg.isInstanceOf[ProdConsBoundedBufferConfig.ProduceDataMessage]) {
          if (itemsProduced == numItemsToProduce) {
            exit()
          } else {
            produceData()
          }
        } else {
          val ex = new IllegalArgumentException("Unsupported message: " + theMsg)
          ex.printStackTrace(System.err)
        }
      }

      override def onPreExit() {
        manager.send(MessageSource.PRODUCER, ProducerExitMessage.ONLY)
      }
    }

    private class ConsumerActor(id: Int, manager: ManagerActor) extends HabaneroActor[AnyRef] {

      private val consumerAvailableMessage = new ProdConsBoundedBufferConfig.ConsumerAvailableMessage(this)
      private var consItem: Double = 0

      override def onPostStart() {
        manager.send(MessageSource.CONSUMER, consumerAvailableMessage)
      }

      protected def consumeDataItem(dataToConsume: Double) {
        consItem = processItem(consItem + dataToConsume, consCost)
      }

      override def process(theMsg: AnyRef) {
        theMsg match {
          case dm: ProdConsBoundedBufferConfig.DataItemMessage =>
            consumeDataItem(dm.data)
            manager.send(MessageSource.CONSUMER, consumerAvailableMessage)
          case _: ProdConsBoundedBufferConfig.ConsumerExitMessage =>
            exit()
          case msg =>
            val ex = new IllegalArgumentException("Unsupported message: " + msg)
            ex.printStackTrace(System.err)
        }
      }
    }

  }

}
