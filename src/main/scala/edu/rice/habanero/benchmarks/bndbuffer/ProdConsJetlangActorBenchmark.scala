package edu.rice.habanero.benchmarks.bndbuffer

import edu.rice.habanero.actors.{JetlangActor, JetlangActorState, JetlangPool}
import edu.rice.habanero.benchmarks.bndbuffer.ProdConsBoundedBufferConfig._
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}

import scala.collection.mutable.ListBuffer

/**
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object ProdConsJetlangActorBenchmark {
  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new ProdConsJetlangActorBenchmark)
  }

  private final class ProdConsJetlangActorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      ProdConsBoundedBufferConfig.parseArgs(args)
    }

    def printArgInfo() {
      ProdConsBoundedBufferConfig.printArgs()
    }

    def runIteration() {
      val manager = new ManagerActor(
        ProdConsBoundedBufferConfig.bufferSize,
        ProdConsBoundedBufferConfig.numProducers,
        ProdConsBoundedBufferConfig.numConsumers,
        ProdConsBoundedBufferConfig.numItemsPerProducer)

      manager.start()

      JetlangActorState.awaitTermination()
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
      if (lastIteration) {
        JetlangPool.shutdown()
      }
    }

    private class ManagerActor(bufferSize: Int, numProducers: Int, numConsumers: Int, numItemsPerProducer: Int) extends JetlangActor[AnyRef] {


      private val adjustedBufferSize: Int = bufferSize - numProducers
      private val availableProducers = new ListBuffer[ProducerActor]
      private val availableConsumers = new ListBuffer[ConsumerActor]
      private val pendingData = new ListBuffer[ProdConsBoundedBufferConfig.DataItemMessage]
      private var numTerminatedProducers: Int = 0

      private val producers = Array.tabulate[ProducerActor](numProducers)(i =>
        new ProducerActor(i, this, numItemsPerProducer))
      private val consumers = Array.tabulate[ConsumerActor](numConsumers)(i =>
        new ConsumerActor(i, this))

      override def onPostStart() {
        consumers.foreach(loopConsumer => {
          availableConsumers.append(loopConsumer)
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

      override def process(theMsg: AnyRef) {
        theMsg match {
          case dm: ProdConsBoundedBufferConfig.DataItemMessage =>
            val producer: ProducerActor = dm.producer.asInstanceOf[ProducerActor]
            if (availableConsumers.isEmpty) {
              pendingData.append(dm)
            } else {
              availableConsumers.remove(0).send(dm)
            }
            if (pendingData.size >= adjustedBufferSize) {
              availableProducers.append(producer)
            } else {
              producer.send(ProduceDataMessage.ONLY)
            }
          case cm: ProdConsBoundedBufferConfig.ConsumerAvailableMessage =>
            val consumer: ConsumerActor = cm.consumer.asInstanceOf[ConsumerActor]
            if (pendingData.isEmpty) {
              availableConsumers.append(consumer)
              tryExit()
            } else {
              consumer.send(pendingData.remove(0))
              if (!availableProducers.isEmpty) {
                availableProducers.remove(0).send(ProduceDataMessage.ONLY)
              }
            }
          case _: ProdConsBoundedBufferConfig.ProducerExitMessage =>
            numTerminatedProducers += 1
            tryExit()
          case msg =>
            val ex = new IllegalArgumentException("Unsupported message: " + msg)
            ex.printStackTrace(System.err)
        }
      }

      def tryExit() {
        if (numTerminatedProducers == numProducers && availableConsumers.size == numConsumers) {
          exit()
        }
      }
    }

    private class ProducerActor(id: Int, manager: ManagerActor, numItemsToProduce: Int) extends JetlangActor[AnyRef] {

      private var prodItem: Double = 0.0
      private var itemsProduced: Int = 0

      private def produceData() {
        prodItem = processItem(prodItem, prodCost)
        manager.send(new ProdConsBoundedBufferConfig.DataItemMessage(prodItem, this))
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
        manager.send(ProducerExitMessage.ONLY)
      }
    }

    private class ConsumerActor(id: Int, manager: ManagerActor) extends JetlangActor[AnyRef] {

      private val consumerAvailableMessage = new ProdConsBoundedBufferConfig.ConsumerAvailableMessage(this)
      private var consItem: Double = 0

      protected def consumeDataItem(dataToConsume: Double) {
        consItem = processItem(consItem + dataToConsume, consCost)
      }

      override def process(theMsg: AnyRef) {
        theMsg match {
          case dm: ProdConsBoundedBufferConfig.DataItemMessage =>
            consumeDataItem(dm.data)
            manager.send(consumerAvailableMessage)
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
