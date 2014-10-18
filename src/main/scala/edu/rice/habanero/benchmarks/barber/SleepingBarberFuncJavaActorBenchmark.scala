package edu.rice.habanero.benchmarks.barber

import java.util.Random
import java.util.concurrent.atomic.AtomicLong

import edu.rice.habanero.actors.{FuncJavaActor, FuncJavaActorState, FuncJavaPool}
import edu.rice.habanero.benchmarks.barber.SleepingBarberConfig._
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}

import scala.collection.mutable.ListBuffer

/**
 * source: https://code.google.com/p/gparallelizer/wiki/ActorsExamples
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object SleepingBarberFuncJavaActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new SleepingBarberFuncJavaActorBenchmark)
  }

  private final class SleepingBarberFuncJavaActorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      SleepingBarberConfig.parseArgs(args)
    }

    def printArgInfo() {
      SleepingBarberConfig.printArgs()
    }

    def runIteration() {

      val idGenerator = new AtomicLong(0)

      val barber = new BarberActor()
      val room = new WaitingRoomActor(SleepingBarberConfig.W, barber)
      val factoryActor = new CustomerFactoryActor(idGenerator, SleepingBarberConfig.N, room)

      barber.start()
      room.start()
      factoryActor.start()

      factoryActor.send(Start.ONLY)

      FuncJavaActorState.awaitTermination()

      track("CustomerAttempts", idGenerator.get())
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double): Unit = {
      if (lastIteration) {
        FuncJavaPool.shutdown()
      }
    }
  }


  private case class Enter(customer: FuncJavaActor[AnyRef], room: FuncJavaActor[AnyRef])

  private case class Returned(customer: FuncJavaActor[AnyRef])


  private class WaitingRoomActor(capacity: Int, barber: BarberActor) extends FuncJavaActor[AnyRef] {

    private val self = this
    private val waitingCustomers = new ListBuffer[FuncJavaActor[AnyRef]]()
    private var barberAsleep = true

    override def process(msg: AnyRef) {
      msg match {
        case message: Enter =>

          val customer = message.customer
          if (waitingCustomers.size == capacity) {

            customer.send(Full.ONLY)

          } else {

            waitingCustomers.append(customer)
            if (barberAsleep) {

              barberAsleep = false
              self.send(Next.ONLY)

            } else {

              customer.send(Wait.ONLY)
            }
          }

        case message: Next =>

          if (waitingCustomers.size > 0) {

            val customer = waitingCustomers.remove(0)
            barber.send(new Enter(customer, self))

          } else {

            barber.send(Wait.ONLY)
            barberAsleep = true

          }

        case message: Exit =>

          barber.send(Exit.ONLY)
          exit()

      }
    }
  }

  private class BarberActor extends FuncJavaActor[AnyRef] {

    private val random = new Random()

    override def process(msg: AnyRef) {
      msg match {
        case message: Enter =>

          val customer = message.customer
          val room = message.room

          customer.send(Start.ONLY)
          // println("Barber: Processing customer " + customer)
          SleepingBarberConfig.busyWait(random.nextInt(SleepingBarberConfig.AHR) + 10)
          customer.send(Done.ONLY)
          room.send(Next.ONLY)

        case message: Wait =>

        // println("Barber: No customers. Going to have a sleep")

        case message: Exit =>

          exit()

      }
    }
  }

  private class CustomerFactoryActor(idGenerator: AtomicLong, haircuts: Int, room: WaitingRoomActor) extends FuncJavaActor[AnyRef] {

    private val self = this
    private val random = new Random()
    private var numHairCutsSoFar = 0

    override def process(msg: AnyRef) {
      msg match {
        case message: Start =>

          var i = 0
          while (i < haircuts) {
            sendCustomerToRoom()
            SleepingBarberConfig.busyWait(random.nextInt(SleepingBarberConfig.APR) + 10)
            i += 1
          }

        case message: Returned =>

          idGenerator.incrementAndGet()
          sendCustomerToRoom(message.customer)

        case message: Done =>

          numHairCutsSoFar += 1
          if (numHairCutsSoFar == haircuts) {
            println("Total attempts: " + idGenerator.get())
            room.send(Exit.ONLY)
            exit()
          }
      }
    }

    private def sendCustomerToRoom() {
      val customer = new CustomerActor(idGenerator.incrementAndGet(), self)
      customer.start()

      sendCustomerToRoom(customer)
    }

    private def sendCustomerToRoom(customer: FuncJavaActor[AnyRef]) {
      val enterMessage = new Enter(customer, room)
      room.send(enterMessage)
    }
  }

  private class CustomerActor(val id: Long, factoryActor: CustomerFactoryActor) extends FuncJavaActor[AnyRef] {

    private val self = this

    override def process(msg: AnyRef) {
      msg match {
        case message: Full =>

          // println("Customer-" + id + " The waiting room is full. I am leaving.")
          factoryActor.send(new Returned(self))

        case message: Wait =>

        // println("Customer-" + id + " I will wait.")

        case message: Start =>

        // println("Customer-" + id + " I am now being served.")

        case message: Done =>

          //  println("Customer-" + id + " I have been served.")
          factoryActor.send(Done.ONLY)
          exit()
      }
    }
  }

}
