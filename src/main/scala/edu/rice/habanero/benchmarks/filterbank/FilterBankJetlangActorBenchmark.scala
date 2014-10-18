package edu.rice.habanero.benchmarks.filterbank

import java.util

import edu.rice.habanero.actors._
import edu.rice.habanero.benchmarks.filterbank.FilterBankConfig.{BootMessage, ExitMessage, NextMessage}
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}

import scala.collection.JavaConversions._

/**
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object FilterBankJetlangActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new FilterBankJetlangActorBenchmark)
  }

  private final class FilterBankJetlangActorBenchmark extends Benchmark {
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

      // create the pipeline of actors
      val producer = new ProducerActor(numSimulations)
      val sink = new SinkActor(sinkPrintRate)
      val combine = new CombineActor(sink)
      val integrator = new IntegratorActor(numChannels, combine)
      val branches = new BranchesActor(numChannels, numColumns, H, F, integrator)
      val source = new SourceActor(producer, branches)

      // start the actors
      producer.start()
      source.start()

      // start the pipeline
      producer.send(new NextMessage(source))

      JetlangActorState.awaitTermination()
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
      if (lastIteration) {
        JetlangPool.shutdown()
      }
    }
  }

  private class ProducerActor(numSimulations: Int) extends JetlangActor[FilterBankConfig.Message] {

    private var numMessagesSent: Int = 0

    override def process(theMsg: FilterBankConfig.Message) {
      theMsg match {
        case message: FilterBankConfig.NextMessage =>
          val source = message.source.asInstanceOf[JetlangActor[FilterBankConfig.Message]]
          if (numMessagesSent == numSimulations) {
            source.send(ExitMessage.ONLY)
            exit()
          }
          else {
            source.send(BootMessage.ONLY)
            numMessagesSent += 1
          }
        case message =>
          val ex = new IllegalArgumentException("Unsupported message: " + message)
          ex.printStackTrace(System.err)
      }
    }
  }

  private abstract class FilterBankActor(nextActor: JetlangActor[FilterBankConfig.Message]) extends JetlangActor[FilterBankConfig.Message] {

    protected override def onPostStart() {
      if (!nextActor.hasStarted()) {
        nextActor.start()
      }
    }

    protected override def onPostExit() {
      nextActor.send(ExitMessage.ONLY)
    }
  }

  private class SourceActor(producer: JetlangActor[FilterBankConfig.Message], nextActor: JetlangActor[FilterBankConfig.Message]) extends FilterBankActor(nextActor) {

    private final val maxValue: Int = 1000
    private var current: Int = 0

    override def process(theMsg: FilterBankConfig.Message) {
      theMsg match {
        case _: FilterBankConfig.BootMessage =>
          nextActor.send(new FilterBankConfig.ValueMessage(current))
          current = (current + 1) % maxValue
          producer.send(new FilterBankConfig.NextMessage(this))
        case _: FilterBankConfig.ExitMessage =>
          exit()
        case message =>
          val ex = new IllegalArgumentException("Unsupported message: " + message)
          ex.printStackTrace(System.err)
      }
    }
  }

  private class SinkActor(printRate: Int) extends JetlangActor[FilterBankConfig.Message] {

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

  private class BranchesActor(numChannels: Int, numColumns: Int, H: Array[Array[Double]], F: Array[Array[Double]], nextActor: IntegratorActor) extends JetlangActor[FilterBankConfig.Message] {

    private final val banks = Array.tabulate[BankActor](numChannels)(i => {
      new BankActor(i, numColumns, H(i), F(i), nextActor)
    })

    protected override def onPostStart() {
      for (loopBank <- banks) {
        loopBank.start()
      }
    }

    protected override def onPostExit() {
      for (loopBank <- banks) {
        loopBank.send(ExitMessage.ONLY)
      }
    }

    override def process(theMsg: FilterBankConfig.Message) {
      theMsg match {
        case _: FilterBankConfig.ValueMessage =>
          for (loopBank <- banks) {
            loopBank.send(theMsg)
          }
        case _: FilterBankConfig.ExitMessage =>
          exit()
        case message =>
          val ex = new IllegalArgumentException("Unsupported message: " + message)
          ex.printStackTrace(System.err)
      }
    }
  }

  private class BankActor(sourceId: Int, numColumns: Int, H: Array[Double], F: Array[Double], integrator: IntegratorActor) extends JetlangActor[FilterBankConfig.Message] {

    private final val firstActor = new DelayActor(sourceId + ".1", numColumns - 1,
      new FirFilterActor(sourceId + ".1", numColumns, H,
        new SampleFilterActor(numColumns,
          new DelayActor(sourceId + ".2", numColumns - 1,
            new FirFilterActor(sourceId + ".2", numColumns, F,
              new TaggedForwardActor(sourceId, integrator))))))

    protected override def onPostStart() {
      firstActor.start()
    }

    protected override def onPostExit() {
      firstActor.send(ExitMessage.ONLY)
    }

    override def process(theMsg: FilterBankConfig.Message) {
      theMsg match {
        case _: FilterBankConfig.ValueMessage =>
          firstActor.send(theMsg)
        case _: FilterBankConfig.ExitMessage =>
          exit()
        case message =>
          val ex = new IllegalArgumentException("Unsupported message: " + message)
          ex.printStackTrace(System.err)
      }
    }
  }

  private class DelayActor(sourceId: String, delayLength: Int, nextActor: FirFilterActor) extends FilterBankActor(nextActor) {

    private final val state = Array.tabulate[Double](delayLength)(i => 0)
    private var placeHolder: Int = 0

    override def process(theMsg: FilterBankConfig.Message) {
      theMsg match {
        case message: FilterBankConfig.ValueMessage =>
          val result: Double = message.value
          nextActor.send(new FilterBankConfig.ValueMessage(state(placeHolder)))
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

  private class FirFilterActor(sourceId: String, peekLength: Int, coefficients: Array[Double], nextActor: JetlangActor[FilterBankConfig.Message]) extends FilterBankActor(nextActor) {

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
            nextActor.send(new FilterBankConfig.ValueMessage(sum))
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

  private class SampleFilterActor(sampleRate: Int, nextActor: JetlangActor[FilterBankConfig.Message]) extends FilterBankActor(nextActor) {

    private var samplesReceived: Int = 0

    override def process(theMsg: FilterBankConfig.Message) {
      theMsg match {
        case _: FilterBankConfig.ValueMessage =>
          if (samplesReceived == 0) {
            nextActor.send(theMsg)
          } else {
            nextActor.send(SampleFilterActor.ZERO_RESULT)
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

  private class TaggedForwardActor(sourceId: Int, nextActor: IntegratorActor) extends FilterBankActor(nextActor) {

    override def process(theMsg: FilterBankConfig.Message) {
      theMsg match {
        case message: FilterBankConfig.ValueMessage =>
          val result: Double = message.value
          nextActor.send(new FilterBankConfig.SourcedValueMessage(sourceId, result))
        case _: FilterBankConfig.ExitMessage =>
          exit()
        case message =>
          val ex = new IllegalArgumentException("Unsupported message: " + message)
          ex.printStackTrace(System.err)
      }
    }
  }

  private class IntegratorActor(numChannels: Int, nextActor: JetlangActor[FilterBankConfig.Message]) extends FilterBankActor(nextActor) {

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
            nextActor.send(new FilterBankConfig.CollectionMessage[Double](firstMap.values))
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

  private class CombineActor(nextActor: JetlangActor[FilterBankConfig.Message]) extends FilterBankActor(nextActor) {

    override def process(theMsg: FilterBankConfig.Message) {
      theMsg match {
        case message: FilterBankConfig.CollectionMessage[_] =>
          val result = message.values.asInstanceOf[util.Collection[Double]]
          var sum: Double = 0
          for (loopValue <- result) {
            sum += loopValue
          }
          nextActor.send(new FilterBankConfig.ValueMessage(sum))
        case _: FilterBankConfig.ExitMessage =>
          exit()
        case message =>
          val ex = new IllegalArgumentException("Unsupported message: " + message)
          ex.printStackTrace(System.err)
      }
    }
  }

}
