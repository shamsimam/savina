package edu.rice.habanero.benchmarks.bitonicsort

import edu.rice.habanero.actors.{GparsActor, GparsActorState, GparsPool}
import edu.rice.habanero.benchmarks.philosopher.PhilosopherAkkaActorBenchmark.ExitMessage
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner, PseudoRandom}

import scala.collection.mutable.ListBuffer

/**
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object BitonicSortGparsActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new BitonicSortGparsActorBenchmark)
  }

  private final class BitonicSortGparsActorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      BitonicSortConfig.parseArgs(args)
    }

    def printArgInfo() {
      BitonicSortConfig.printArgs()
    }

    def runIteration() {

      val validationActor = new ValidationActor(BitonicSortConfig.N)
      validationActor.start()

      val adapterActor = new DataValueAdapterActor(validationActor)
      adapterActor.start()

      val kernelActor = new BitonicSortKernelActor(BitonicSortConfig.N, true, adapterActor)
      kernelActor.start()

      val sourceActor = new IntSourceActor(BitonicSortConfig.N, BitonicSortConfig.M, BitonicSortConfig.S, kernelActor)
      sourceActor.start()

      sourceActor.send(StartMessage())

      GparsActorState.awaitTermination()
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double): Unit = {
      if (lastIteration) {
        GparsPool.shutdown()
      }
    }
  }

  private case class NextActorMessage(actor: GparsActor[AnyRef])

  private case class ValueMessage(value: Long)

  private case class DataMessage(orderId: Int, value: Long)

  private case class StartMessage()


  private class ValueDataAdapterActor(orderId: Int, nextActor: GparsActor[AnyRef]) extends GparsActor[AnyRef] {
    override def process(msg: AnyRef) {
      msg match {
        case vm: ValueMessage =>

          nextActor.send(new DataMessage(orderId, vm.value))

        case dm: DataMessage =>

          nextActor.send(dm)

        case em: ExitMessage =>

          nextActor.send(em)
          exit()
      }
    }
  }

  private class DataValueAdapterActor(nextActor: GparsActor[AnyRef]) extends GparsActor[AnyRef] {
    override def process(msg: AnyRef) {
      msg match {
        case vm: ValueMessage =>

          nextActor.send(vm)

        case dm: DataMessage =>

          nextActor.send(new ValueMessage(dm.value))

        case em: ExitMessage =>

          nextActor.send(em)
          exit()
      }
    }
  }

  private class RoundRobinSplitterActor(name: String, length: Int, receivers: Array[GparsActor[AnyRef]]) extends GparsActor[AnyRef] {

    private var receiverIndex = 0
    private var currentRun = 0

    override def process(msg: AnyRef) {
      msg match {
        case vm: ValueMessage =>

          receivers(receiverIndex).send(vm)
          currentRun += 1
          if (currentRun == length) {
            receiverIndex = (receiverIndex + 1) % receivers.length
            currentRun = 0
          }

        case em: ExitMessage =>

          receivers.foreach(loopActor => loopActor.send(em))
          exit()
      }
    }
  }

  private class RoundRobinJoinerActor(name: String, length: Int, numJoiners: Int, nextActor: GparsActor[AnyRef]) extends GparsActor[AnyRef] {

    private val receivedData = Array.tabulate[ListBuffer[DataMessage]](numJoiners)(i => new ListBuffer[DataMessage]())

    private var forwardIndex = 0
    private var currentRun = 0

    private var exitsReceived = 0

    override def process(msg: AnyRef) {
      msg match {
        case dm: DataMessage =>

          receivedData(dm.orderId).append(dm)
          tryForwardMessages(dm)

        case em: ExitMessage =>

          exitsReceived += 1
          if (exitsReceived == numJoiners) {
            nextActor.send(em)
            exit()
          }
      }
    }

    def tryForwardMessages(dm: DataMessage) {
      while (receivedData(forwardIndex).nonEmpty) {
        val dm = receivedData(forwardIndex).remove(0)
        val vm = new ValueMessage(dm.value)
        nextActor.send(vm)
        currentRun += 1
        if (currentRun == length) {
          forwardIndex = (forwardIndex + 1) % numJoiners
          currentRun = 0
        }
      }
    }
  }

  /**
   * Compares the two input keys and exchanges their order if they are not sorted.
   *
   * sortDirection determines if the sort is nondecreasing (UP) [true] or nonincreasing (DOWN) [false].
   */
  private class CompareExchangeActor(orderId: Int, sortDirection: Boolean, nextActor: GparsActor[AnyRef]) extends GparsActor[AnyRef] {

    private var k1: Long = 0
    private var valueAvailable = false

    override def process(msg: AnyRef) {
      msg match {
        case vm: ValueMessage =>

          if (!valueAvailable) {

            valueAvailable = true
            k1 = vm.value

          } else {

            valueAvailable = false
            val k2 = vm.value
            val (minK, maxK) = if (k1 <= k2) (k1, k2) else (k2, k1)
            if (sortDirection) {
              // UP sort
              nextActor.send(DataMessage(orderId, minK))
              nextActor.send(DataMessage(orderId, maxK))
            } else {
              // DOWN sort
              nextActor.send(DataMessage(orderId, maxK))
              nextActor.send(DataMessage(orderId, minK))
            }

          }

        case em: ExitMessage =>

          nextActor.send(em)
          exit()
      }
    }
  }

  /**
   * Partition the input bitonic sequence of length L into two bitonic sequences of length L/2,
   * with all numbers in the first sequence <= all numbers in the second sequence if sortdir is UP (similar case for DOWN sortdir)
   *
   * Graphically, it is a bunch of CompareExchanges with same sortdir, clustered together in the sort network at a particular step (of some merge stage).
   */
  private class PartitionBitonicSequenceActor(orderId: Int, length: Int, sortDir: Boolean, nextActor: GparsActor[AnyRef]) extends GparsActor[AnyRef] {

    val halfLength = length / 2
    val forwardActor = {
      val actor = new ValueDataAdapterActor(orderId, nextActor)
      actor.start()
      actor
    }
    val joinerActor = {
      val actor = new RoundRobinJoinerActor("Partition-" + orderId, 1, halfLength, forwardActor)
      actor.start()
      actor
    }
    val workerActors = Array.tabulate[GparsActor[AnyRef]](halfLength)(i => {
      val actor = new CompareExchangeActor(i, sortDir, joinerActor)
      actor.start()
      actor
    })
    val splitterActor = {
      val actor = new RoundRobinSplitterActor("Partition-" + orderId, 1, workerActors)
      actor.start()
      actor
    }


    override def process(msg: AnyRef) {
      msg match {
        case vm: ValueMessage =>

          splitterActor.send(vm)

        case em: ExitMessage =>

          splitterActor.send(em)
          exit()
      }
    }
  }

  /**
   * One step of a particular merge stage (used by all merge stages except the last)
   *
   * directionCounter determines which step we are in the current merge stage (which in turn is determined by <L, numSeqPartitions>)
   */
  private class StepOfMergeActor(orderId: Int, length: Int, numSeqPartitions: Int, directionCounter: Int, nextActor: GparsActor[AnyRef]) extends GparsActor[AnyRef] {

    val forwardActor = {
      val actor = new DataValueAdapterActor(nextActor)
      actor.start()
      actor
    }
    val joinerActor = {
      val actor = new RoundRobinJoinerActor("StepOfMerge-" + orderId + ":" + length, length, numSeqPartitions, forwardActor)
      actor.start()
      actor
    }
    val workerActors = Array.tabulate[GparsActor[AnyRef]](numSeqPartitions)(i => {
      // finding out the currentDirection is a bit tricky -
      // the direction depends only on the subsequence number during the FIRST step.
      // So to determine the FIRST step subsequence to which this sequence belongs, divide this sequence's number j by directionCounter
      // (bcoz 'directionCounter' tells how many subsequences of the current step make up one subsequence of the FIRST step).
      // Then, test if that result is even or odd to determine if currentDirection is UP or DOWN respectively.
      val currentDirection = (i / directionCounter) % 2 == 0

      // The last step needs special care to avoid split-joins with just one branch.
      if (length > 2) {
        val actor = new PartitionBitonicSequenceActor(i, length, currentDirection, joinerActor)
        actor.start()
        actor
      } else {
        // PartitionBitonicSequence of the last step (L=2) is simply a CompareExchange
        val actor = new CompareExchangeActor(i, currentDirection, joinerActor)
        actor.start()
        actor
      }
    })
    val splitterActor = {
      val actor = new RoundRobinSplitterActor("StepOfMerge-" + orderId + ":" + length, length, workerActors)
      actor.start()
      actor
    }


    override def process(msg: AnyRef) {
      msg match {
        case vm: ValueMessage =>

          splitterActor.send(vm)

        case em: ExitMessage =>

          splitterActor.send(em)
          exit()
      }
    }
  }

  /**
   * One step of the last merge stage
   *
   * Main difference form StepOfMerge is the direction of sort.
   * It is always in the same direction - sortdir.
   */
  private class StepOfLastMergeActor(length: Int, numSeqPartitions: Int, sortDirection: Boolean, nextActor: GparsActor[AnyRef]) extends GparsActor[AnyRef] {

    val joinerActor = {
      val actor = new RoundRobinJoinerActor("StepOfLastMerge-" + length, length, numSeqPartitions, nextActor)
      actor.start()
      actor
    }
    val workerActors = Array.tabulate[GparsActor[AnyRef]](numSeqPartitions)(i => {
      // The last step needs special care to avoid split-joins with just one branch.
      if (length > 2) {
        val actor = new PartitionBitonicSequenceActor(i, length, sortDirection, joinerActor)
        actor.start()
        actor
      } else {
        // PartitionBitonicSequence of the last step (L=2) is simply a CompareExchange
        val actor = new CompareExchangeActor(i, sortDirection, joinerActor)
        actor.start()
        actor
      }
    })
    val splitterActor = {
      val actor = new RoundRobinSplitterActor("StepOfLastMerge-" + length, length, workerActors)
      actor.start()
      actor
    }


    override def process(msg: AnyRef) {
      msg match {
        case vm: ValueMessage =>

          splitterActor.send(vm)

        case em: ExitMessage =>

          splitterActor.send(em)
          exit()
      }
    }
  }

  /**
   * Divide the input sequence of length N into subsequences of length P and sort each of them
   * (either UP or DOWN depending on what subsequence number [0 to N/P-1] they get.
   * All even subsequences are sorted UP and all odd subsequences are sorted DOWN).
   * In short, a MergeStage is N/P Bitonic Sorters of order P each.
   * But, this MergeStage is implemented *iteratively* as logP STEPS.
   */
  private class MergeStageActor(P: Int, N: Int, nextActor: GparsActor[AnyRef]) extends GparsActor[AnyRef] {

    val forwardActor = {
      var loopActor: GparsActor[AnyRef] = nextActor

      // for each of the lopP steps (except the last step) of this merge stage
      var i = P / 2
      while (i >= 1) {

        // length of each sequence for the current step - goes like P, P/2, ..., 2.
        val L = P / i
        // numSeqPartitions is the number of PartitionBitonicSequence-rs in this step
        val numSeqPartitions = (N / P) * i
        val directionCounter = i

        val tempActor = new StepOfMergeActor(i, L, numSeqPartitions, directionCounter, loopActor)
        tempActor.start()
        loopActor = tempActor

        i /= 2
      }

      loopActor
    }

    override def process(msg: AnyRef) {
      msg match {
        case vm: ValueMessage =>

          forwardActor.send(vm)

        case em: ExitMessage =>

          forwardActor.send(em)
          exit()
      }
    }
  }

  /**
   * The LastMergeStage is basically one Bitonic Sorter of order N i.e.,
   * it takes the bitonic sequence produced by the previous merge stages
   * and applies a bitonic merge on it to produce the final sorted sequence.
   *
   * This is implemented iteratively as logN steps.
   */
  private class LastMergeStageActor(N: Int, sortDirection: Boolean, nextActor: GparsActor[AnyRef]) extends GparsActor[AnyRef] {

    val forwardActor = {
      var loopActor: GparsActor[AnyRef] = nextActor

      // for each of the lopN steps (except the last step) of this merge stage
      var i = N / 2
      while (i >= 1) {

        // length of each sequence for the current step - goes like N, N/2, ..., 2.
        val L = N / i
        // numSeqPartitions is the number of PartitionBitonicSequence-rs in this step
        val numSeqPartitions = i

        val tempActor = new StepOfLastMergeActor(L, numSeqPartitions, sortDirection, loopActor)
        tempActor.start()
        loopActor = tempActor

        i /= 2
      }

      loopActor
    }

    override def process(msg: AnyRef) {
      msg match {
        case vm: ValueMessage =>

          forwardActor.send(vm)

        case em: ExitMessage =>

          forwardActor.send(em)
          exit()
      }
    }
  }

  /**
   * The top-level kernel of bitonic-sort (iterative version) -
   * It has logN merge stages and all merge stages except the last progressively builds a bitonic sequence out of the input sequence.
   * The last merge stage acts on the resultant bitonic sequence to produce the final sorted sequence (sortdir determines if it is UP or DOWN).
   */
  private class BitonicSortKernelActor(N: Int, sortDirection: Boolean, nextActor: GparsActor[AnyRef]) extends GparsActor[AnyRef] {

    val forwardActor = {
      var loopActor: GparsActor[AnyRef] = nextActor

      {
        val tempActor = new LastMergeStageActor(N, sortDirection, loopActor)
        tempActor.start()
        loopActor = tempActor
      }

      var i = N / 2
      while (i >= 2) {

        val tempActor = new MergeStageActor(i, N, loopActor)
        tempActor.start()
        loopActor = tempActor

        i /= 2
      }

      loopActor
    }

    override def process(msg: AnyRef) {
      msg match {
        case vm: ValueMessage =>

          forwardActor.send(vm)

        case em: ExitMessage =>

          forwardActor.send(em)
          exit()
      }
    }
  }

  private class IntSourceActor(numValues: Int, maxValue: Long, seed: Long, nextActor: GparsActor[AnyRef]) extends GparsActor[AnyRef] {

    private val random = new PseudoRandom(seed)
    private val sb = new StringBuilder()

    override def process(msg: AnyRef) {

      msg match {
        case nm: StartMessage =>

          var i = 0
          while (i < numValues) {

            val candidate = Math.abs(random.nextLong()) % maxValue
            if (BitonicSortConfig.debug) {
              sb.append(candidate + " ")
            }
            val message = new ValueMessage(candidate)
            nextActor.send(message)

            i += 1
          }
          if (BitonicSortConfig.debug) {
            println("  SOURCE: " + sb)
          }

          nextActor.send(ExitMessage())
          exit()
      }
    }
  }

  private class ValidationActor(numValues: Int) extends GparsActor[AnyRef] {

    private var sumSoFar = 0.0
    private var valuesSoFar = 0
    private var prevValue = 0L
    private var errorValue = (-1L, -1)
    private val sb = new StringBuilder()

    override def process(msg: AnyRef) {

      msg match {
        case vm: ValueMessage =>

          valuesSoFar += 1

          if (BitonicSortConfig.debug) {
            sb.append(vm.value + " ")
          }
          if (vm.value < prevValue && errorValue._1 < 0) {
            errorValue = (vm.value, valuesSoFar - 1)
          }
          prevValue = vm.value
          sumSoFar += prevValue

        case em: ExitMessage =>

          if (valuesSoFar == numValues) {
            if (BitonicSortConfig.debug) {
              println("  OUTPUT: " + sb)
            }
            if (errorValue._1 >= 0) {
              println("  ERROR: Value out of place: " + errorValue._1 + " at index " + errorValue._2)
            } else {
              println("  Elements sum: " + sumSoFar)
            }
          } else {
            println("  ERROR: early exit triggered, received only " + valuesSoFar + " values!")
          }
          exit()
      }
    }
  }

}
