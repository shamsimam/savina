package edu.rice.habanero.benchmarks.filterbank

import java.util

import akka.actor.{ActorRef, Props}
import edu.rice.habanero.actors._
import edu.rice.habanero.benchmarks.filterbank.FilterBankConfig.{BootMessage, ExitMessage, NextMessage}
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}

import scala.collection.JavaConversions._

/**
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object FilterBankAkkaActorBenchmark {

  def main(args: Array[String]) {
    println("WARNING: AKKA version DEADLOCKS. Needs debugging!")
    BenchmarkRunner.runBenchmark(args, new FilterBankAkkaActorBenchmark)
  }

  private final class FilterBankAkkaActorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      FilterBankConfig.parseArgs(args)
    }

    def printArgInfo() {
      FilterBankConfig.printArgs()
    }

    def runIteration() {
      val numSimulations: Int = FilterBankConfig.NUM_SIMULATIONS
      val numChannels: Int = FilterBankConfig.NUM_CHANNELS
      val numColumns: Int = FilterBankConfig.NUM_COLUMNS
      val H: Array[Array[Double]] = FilterBankConfig.H
      val F: Array[Array[Double]] = FilterBankConfig.F
      val sinkPrintRate: Int = FilterBankConfig.SINK_PRINT_RATE

      val system = AkkaActorState.newActorSystem("FilterBank")

      // create the pipeline of actors
      val producer = system.actorOf(Props(new ProducerActor(numSimulations)))
      val sink = system.actorOf(Props(new SinkActor(sinkPrintRate)))
      val combine = system.actorOf(Props(new CombineActor(sink)))
      val integrator = system.actorOf(Props(new IntegratorActor(numChannels, combine)))
      val branches = system.actorOf(Props(new BranchesActor(numChannels, numColumns, H, F, integrator)))
      val source = system.actorOf(Props(new SourceActor(producer, branches)))

      // start the actors
      AkkaActorState.startActor(producer)
      AkkaActorState.startActor(source)

      // start the pipeline
      producer ! new NextMessage(source)

      AkkaActorState.awaitTermination(system)
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
    }
  }

  private class ProducerActor(numSimulations: Int) extends AkkaActor[FilterBankConfig.Message] {

    private var numMessagesSent: Int = 0

    override def process(theMsg: FilterBankConfig.Message) {
      theMsg match {
        case message: FilterBankConfig.NextMessage =>
          val source = message.source.asInstanceOf[ActorRef]
          if (numMessagesSent == numSimulations) {
            source ! ExitMessage.ONLY
            exit()
          }
          else {
            source ! BootMessage.ONLY
            numMessagesSent += 1
          }
        case message =>
          val ex = new IllegalArgumentException("Unsupported message: " + message)
          ex.printStackTrace(System.err)
      }
    }
  }

  private abstract class FilterBankActor(nextActor: ActorRef) extends AkkaActor[FilterBankConfig.Message] {

    protected override def onPostStart() {
      AkkaActorState.startActor(nextActor)
    }

    protected override def onPostExit() {
      nextActor ! ExitMessage.ONLY
    }
  }

  private class SourceActor(producer: ActorRef, nextActor: ActorRef) extends FilterBankActor(nextActor) {

    private final val maxValue: Int = 1000
    private var current: Int = 0

    override def process(theMsg: FilterBankConfig.Message) {
      theMsg match {
        case _: FilterBankConfig.BootMessage =>
          nextActor ! new FilterBankConfig.ValueMessage(current)
          current = (current + 1) % maxValue
          producer ! new FilterBankConfig.NextMessage(self)
        case _: FilterBankConfig.ExitMessage =>
          exit()
        case message =>
          val ex = new IllegalArgumentException("Unsupported message: " + message)
          ex.printStackTrace(System.err)
      }
    }
  }

  private class SinkActor(printRate: Int) extends AkkaActor[FilterBankConfig.Message] {

    private var count: Int = 0

    override def process(theMsg: FilterBankConfig.Message) {
      theMsg match {
        case message: FilterBankConfig.ValueMessage =>
          val result: Double = message.value
          if (FilterBankConfig.debug && (count == 0)) {
            System.out.println("SinkActor: result = " + result)
          }
          count = (count + 1) % printRate
        case _: FilterBankConfig.ExitMessage =>
          exit()
        case message =>
          val ex = new IllegalArgumentException("Unsupported message: " + message)
          ex.printStackTrace(System.err)
      }
    }
  }

  private class BranchesActor(numChannels: Int, numColumns: Int, H: Array[Array[Double]], F: Array[Array[Double]], nextActor: ActorRef) extends AkkaActor[FilterBankConfig.Message] {

    private final val banks = Array.tabulate[ActorRef](numChannels)(i => {
      context.system.actorOf(Props(new BankActor(i, numColumns, H(i), F(i), nextActor)))
    })

    protected override def onPostStart() {
      for (loopBank <- banks) {
        AkkaActorState.startActor(loopBank)
      }
    }

    protected override def onPostExit() {
      for (loopBank <- banks) {
        loopBank ! ExitMessage.ONLY
      }
    }

    override def process(theMsg: FilterBankConfig.Message) {
      theMsg match {
        case _: FilterBankConfig.ValueMessage =>
          for (loopBank <- banks) {
            loopBank ! theMsg
          }
        case _: FilterBankConfig.ExitMessage =>
          exit()
        case message =>
          val ex = new IllegalArgumentException("Unsupported message: " + message)
          ex.printStackTrace(System.err)
      }
    }
  }

  private class BankActor(sourceId: Int, numColumns: Int, H: Array[Double], F: Array[Double], integrator: ActorRef) extends AkkaActor[FilterBankConfig.Message] {

    private final val firstActor = context.system.actorOf(Props(new DelayActor(sourceId + ".1", numColumns - 1,
      context.system.actorOf(Props(new FirFilterActor(sourceId + ".1", numColumns, H,
        context.system.actorOf(Props(new SampleFilterActor(numColumns,
          context.system.actorOf(Props(new DelayActor(sourceId + ".2", numColumns - 1,
            context.system.actorOf(Props(new FirFilterActor(sourceId + ".2", numColumns, F,
              context.system.actorOf(Props(new TaggedForwardActor(sourceId, integrator))))))))))))))))))

    protected override def onPostStart() {
      AkkaActorState.startActor(firstActor)
    }

    protected override def onPostExit() {
      firstActor ! ExitMessage.ONLY
    }

    override def process(theMsg: FilterBankConfig.Message) {
      theMsg match {
        case _: FilterBankConfig.ValueMessage =>
          firstActor ! theMsg
        case _: FilterBankConfig.ExitMessage =>
          exit()
        case message =>
          val ex = new IllegalArgumentException("Unsupported message: " + message)
          ex.printStackTrace(System.err)
      }
    }
  }

  private class DelayActor(sourceId: String, delayLength: Int, nextActor: ActorRef) extends FilterBankActor(nextActor) {

    private final val state = Array.tabulate[Double](delayLength)(i => 0)
    private var placeHolder: Int = 0

    override def process(theMsg: FilterBankConfig.Message) {
      theMsg match {
        case message: FilterBankConfig.ValueMessage =>
          val result: Double = message.value
          nextActor ! new FilterBankConfig.ValueMessage(state(placeHolder))
          state(placeHolder) = result
          placeHolder = (placeHolder + 1) % delayLength
        case _: FilterBankConfig.ExitMessage =>
          exit()
        case message =>
          val ex = new IllegalArgumentException("Unsupported message: " + message)
          ex.printStackTrace(System.err)
      }
    }
  }

  private class FirFilterActor(sourceId: String, peekLength: Int, coefficients: Array[Double], nextActor: ActorRef) extends FilterBankActor(nextActor) {

    private var data = Array.tabulate[Double](peekLength)(i => 0)
    private var dataIndex: Int = 0
    private var dataFull: Boolean = false

    override def process(theMsg: FilterBankConfig.Message) {
      theMsg match {
        case message: FilterBankConfig.ValueMessage =>
          val result: Double = message.value
          data(dataIndex) = result
          dataIndex += 1
          if (dataIndex == peekLength) {
            dataFull = true
            dataIndex = 0
          }
          if (dataFull) {
            var sum: Double = 0.0
            var i: Int = 0
            while (i < peekLength) {
              sum += (data(i) * coefficients(peekLength - i - 1))
              i += 1
            }
            nextActor ! new FilterBankConfig.ValueMessage(sum)
          }
        case _: FilterBankConfig.ExitMessage =>
          exit()
        case _ =>
      }
    }
  }

  private object SampleFilterActor {
    private final val ZERO_RESULT: FilterBankConfig.ValueMessage = new FilterBankConfig.ValueMessage(0)
  }

  private class SampleFilterActor(sampleRate: Int, nextActor: ActorRef) extends FilterBankActor(nextActor) {

    private var samplesReceived: Int = 0

    override def process(theMsg: FilterBankConfig.Message) {
      theMsg match {
        case _: FilterBankConfig.ValueMessage =>
          if (samplesReceived == 0) {
            nextActor ! theMsg
          } else {
            nextActor ! SampleFilterActor.ZERO_RESULT
          }
          samplesReceived = (samplesReceived + 1) % sampleRate
        case _: FilterBankConfig.ExitMessage =>
          exit()
        case message =>
          val ex = new IllegalArgumentException("Unsupported message: " + message)
          ex.printStackTrace(System.err)
      }
    }
  }

  private class TaggedForwardActor(sourceId: Int, nextActor: ActorRef) extends FilterBankActor(nextActor) {

    override def process(theMsg: FilterBankConfig.Message) {
      theMsg match {
        case message: FilterBankConfig.ValueMessage =>
          val result: Double = message.value
          nextActor ! new FilterBankConfig.SourcedValueMessage(sourceId, result)
        case _: FilterBankConfig.ExitMessage =>
          exit()
        case message =>
          val ex = new IllegalArgumentException("Unsupported message: " + message)
          ex.printStackTrace(System.err)
      }
    }
  }

  private class IntegratorActor(numChannels: Int, nextActor: ActorRef) extends FilterBankActor(nextActor) {

    private final val data = new java.util.ArrayList[util.Map[Integer, Double]]
    private var exitsReceived: Int = 0

    override def process(theMsg: FilterBankConfig.Message) {
      theMsg match {
        case message: FilterBankConfig.SourcedValueMessage =>
          val sourceId: Int = message.sourceId
          val result: Double = message.value
          val dataSize: Int = data.size
          var processed: Boolean = false
          var i: Int = 0
          while (i < dataSize) {
            val loopMap: java.util.Map[Integer, Double] = data.get(i)
            if (!loopMap.containsKey(sourceId)) {
              loopMap.put(sourceId, result)
              processed = true
              i = dataSize
            }
            i += 1
          }
          if (!processed) {
            val newMap = new java.util.HashMap[Integer, Double]
            newMap.put(sourceId, result)
            data.add(newMap)
          }
          val firstMap: java.util.Map[Integer, Double] = data.get(0)
          if (firstMap.size == numChannels) {
            nextActor ! new FilterBankConfig.CollectionMessage[Double](firstMap.values)
            data.remove(0)
          }
        case _: FilterBankConfig.ExitMessage =>
          exitsReceived += 1
          if (exitsReceived == numChannels) {
            exit()
          }
        case message =>
          val ex = new IllegalArgumentException("Unsupported message: " + message)
          ex.printStackTrace(System.err)
      }
    }
  }

  private class CombineActor(nextActor: ActorRef) extends FilterBankActor(nextActor) {

    override def process(theMsg: FilterBankConfig.Message) {
      theMsg match {
        case message: FilterBankConfig.CollectionMessage[_] =>
          val result = message.values.asInstanceOf[util.Collection[Double]]
          var sum: Double = 0
          for (loopValue <- result) {
            sum += loopValue
          }
          nextActor ! new FilterBankConfig.ValueMessage(sum)
        case _: FilterBankConfig.ExitMessage =>
          exit()
        case message =>
          val ex = new IllegalArgumentException("Unsupported message: " + message)
          ex.printStackTrace(System.err)
      }
    }
  }

}
