package edu.rice.habanero.benchmarks.bndbuffer

import akka.actor.{ActorRef, Props}
import edu.rice.habanero.actors.{AkkaActor, AkkaActorState}
import edu.rice.habanero.benchmarks.bndbuffer.ProdConsBoundedBufferConfig._
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}

import scala.collection.mutable.ListBuffer

/**
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object ProdConsAkkaActorBenchmark {
  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new ProdConsAkkaActorBenchmark)
  }

  private final class ProdConsAkkaActorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      ProdConsBoundedBufferConfig.parseArgs(args)
    }

    def printArgInfo() {
      ProdConsBoundedBufferConfig.printArgs()
    }

    def runIteration() {

      val system = AkkaActorState.newActorSystem("ProdCons")

      val manager = system.actorOf(Props(
        new ManagerActor(
          ProdConsBoundedBufferConfig.bufferSize,
          ProdConsBoundedBufferConfig.numProducers,
          ProdConsBoundedBufferConfig.numConsumers,
          ProdConsBoundedBufferConfig.numItemsPerProducer)),
        name = "manager")

      AkkaActorState.startActor(manager)

      AkkaActorState.awaitTermination(system)
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
    }

    private class ManagerActor(bufferSize: Int, numProducers: Int, numConsumers: Int, numItemsPerProducer: Int) extends AkkaActor[AnyRef] {


      private val adjustedBufferSize: Int = bufferSize - numProducers
      private val availableProducers = new ListBuffer[ActorRef]
      private val availableConsumers = new ListBuffer[ActorRef]
      private val pendingData = new ListBuffer[ProdConsBoundedBufferConfig.DataItemMessage]
      private var numTerminatedProducers: Int = 0

      private val producers = Array.tabulate[ActorRef](numProducers)(i =>
        context.system.actorOf(Props(new ProducerActor(i, self, numItemsPerProducer))))
      private val consumers = Array.tabulate[ActorRef](numConsumers)(i =>
        context.system.actorOf(Props(new ConsumerActor(i, self))))

      override def onPostStart() {
        consumers.foreach(loopConsumer => {
          availableConsumers.append(loopConsumer)
          AkkaActorState.startActor(loopConsumer)
        })

        producers.foreach(loopProducer => {
          AkkaActorState.startActor(loopProducer)
        })

        producers.foreach(loopProducer => {
          loopProducer ! ProduceDataMessage.ONLY
        })
      }

      override def onPreExit() {
        consumers.foreach(loopConsumer => {
          loopConsumer ! ConsumerExitMessage.ONLY
        })
      }

      override def process(theMsg: AnyRef) {
        theMsg match {
          case dm: ProdConsBoundedBufferConfig.DataItemMessage =>
            val producer: ActorRef = dm.producer.asInstanceOf[ActorRef]
            if (availableConsumers.isEmpty) {
              pendingData.append(dm)
            } else {
              availableConsumers.remove(0) ! dm
            }
            if (pendingData.size >= adjustedBufferSize) {
              availableProducers.append(producer)
            } else {
              producer ! ProduceDataMessage.ONLY
            }
          case cm: ProdConsBoundedBufferConfig.ConsumerAvailableMessage =>
            val consumer: ActorRef = cm.consumer.asInstanceOf[ActorRef]
            if (pendingData.isEmpty) {
              availableConsumers.append(consumer)
              tryExit()
            } else {
              consumer ! pendingData.remove(0)
              if (!availableProducers.isEmpty) {
                availableProducers.remove(0) ! ProduceDataMessage.ONLY
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

    private class ProducerActor(id: Int, manager: ActorRef, numItemsToProduce: Int) extends AkkaActor[AnyRef] {

      private var prodItem: Double = 0.0
      private var itemsProduced: Int = 0

      private def produceData() {
        prodItem = processItem(prodItem, prodCost)
        manager ! new ProdConsBoundedBufferConfig.DataItemMessage(prodItem, self)
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
        manager ! ProducerExitMessage.ONLY
      }
    }

    private class ConsumerActor(id: Int, manager: ActorRef) extends AkkaActor[AnyRef] {

      private val consumerAvailableMessage = new ProdConsBoundedBufferConfig.ConsumerAvailableMessage(self)
      private var consItem: Double = 0

      protected def consumeDataItem(dataToConsume: Double) {
        consItem = processItem(consItem + dataToConsume, consCost)
      }

      override def process(theMsg: AnyRef) {
        theMsg match {
          case dm: ProdConsBoundedBufferConfig.DataItemMessage =>
            consumeDataItem(dm.data)
            manager ! consumerAvailableMessage
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
