package edu.rice.habanero.benchmarks.barber

import java.util.concurrent.atomic.AtomicLong

import edu.rice.habanero.actors.HabaneroSelector
import edu.rice.habanero.benchmarks.barber.SleepingBarberConfig._
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner, PseudoRandom}
import edu.rice.hj.Module0._
import edu.rice.hj.api.HjSuspendable

import scala.collection.mutable.ListBuffer

/**
 * source: https://code.google.com/p/gparallelizer/wiki/SelectorsExamples
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object SleepingBarberHabaneroSelectorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new SleepingBarberHabaneroSelectorBenchmark)
  }

  private final class SleepingBarberHabaneroSelectorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      SleepingBarberConfig.parseArgs(args)
    }

    def printArgInfo() {
      SleepingBarberConfig.printArgs()
    }

    def runIteration() {

      val idGenerator = new AtomicLong(0)
      finish(new HjSuspendable {
        override def run() = {


          val barber = new BarberSelector()
          val room = new WaitingRoomSelector(SleepingBarberConfig.W, barber)
          val factorySelector = new CustomerFactorySelector(idGenerator, SleepingBarberConfig.N, room)

          barber.start()
          room.start()
          factorySelector.start()

          factorySelector.send(0, Start.ONLY)

        }
      })
      track("CustomerAttempts", idGenerator.get())
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
    }
  }


  private case class Enter(customer: HabaneroSelector[AnyRef], room: HabaneroSelector[AnyRef])

  private case class Returned(customer: HabaneroSelector[AnyRef])


  private class WaitingRoomSelector(capacity: Int, barber: BarberSelector) extends HabaneroSelector[AnyRef](1) {

    private val self = this
    private val waitingCustomers = new ListBuffer[HabaneroSelector[AnyRef]]()
    private var barberAsleep = true

    override def process(msg: AnyRef) {
      msg match {
        case message: Enter =>

          val customer = message.customer
          if (waitingCustomers.size == capacity) {

            customer.send(0, Full.ONLY)

          } else {

            waitingCustomers.append(customer)
            if (barberAsleep) {

              barberAsleep = false
              self.send(0, Next.ONLY)

            } else {

              customer.send(0, Wait.ONLY)
            }
          }

        case message: Next =>

          if (waitingCustomers.size > 0) {

            val customer = waitingCustomers.remove(0)
            barber.send(0, new Enter(customer, self))

          } else {

            barber.send(0, Wait.ONLY)
            barberAsleep = true

          }

        case message: Exit =>

          barber.send(0, Exit.ONLY)
          exit()

      }
    }
  }

  private class BarberSelector extends HabaneroSelector[AnyRef](1) {

    private val random = new PseudoRandom()

    override def process(msg: AnyRef) {
      msg match {
        case message: Enter =>

          val customer = message.customer
          val room = message.room

          customer.send(0, Start.ONLY)
          // println("Barber: Processing customer " + customer)
          SleepingBarberConfig.busyWait(random.nextInt(SleepingBarberConfig.AHR) + 10)
          customer.send(0, Done.ONLY)
          room.send(0, Next.ONLY)

        case message: Wait =>

        // println("Barber: No customers. Going to have a sleep")

        case message: Exit =>

          exit()

      }
    }
  }

  private class CustomerFactorySelector(idGenerator: AtomicLong, haircuts: Int, room: WaitingRoomSelector) extends HabaneroSelector[AnyRef](1) {

    private val self = this
    private val random = new PseudoRandom()
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
            room.send(0, Exit.ONLY)
            exit()
          }
      }
    }

    private def sendCustomerToRoom() {
      val customer = new CustomerSelector(idGenerator.incrementAndGet(), self)
      customer.start()

      sendCustomerToRoom(customer)
    }

    private def sendCustomerToRoom(customer: HabaneroSelector[AnyRef]) {
      val enterMessage = new Enter(customer, room)
      room.send(0, enterMessage)
    }
  }

  private class CustomerSelector(val id: Long, factorySelector: CustomerFactorySelector) extends HabaneroSelector[AnyRef](1) {

    private val self = this

    override def process(msg: AnyRef) {
      msg match {
        case message: Full =>

          // println("Customer-" + id + " The waiting room is full. I am leaving.")
          factorySelector.send(0, new Returned(self))

        case message: Wait =>

        // println("Customer-" + id + " I will wait.")

        case message: Start =>

        // println("Customer-" + id + " I am now being served.")

        case message: Done =>

          //  println("Customer-" + id + " I have been served.")
          factorySelector.send(0, Done.ONLY)
          exit()
      }
    }
  }

}
