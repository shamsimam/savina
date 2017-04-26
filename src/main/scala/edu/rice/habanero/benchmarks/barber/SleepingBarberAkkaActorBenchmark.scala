package edu.rice.habanero.benchmarks.barber

import java.util.concurrent.atomic.AtomicLong

import akka.actor.{ActorRef, Props}
import edu.rice.habanero.actors.{AkkaActor, AkkaActorState}
import edu.rice.habanero.benchmarks.barber.SleepingBarberConfig._
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner, PseudoRandom}

import scala.collection.mutable.ListBuffer

/**
 * source: https://code.google.com/p/gparallelizer/wiki/ActorsExamples
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object SleepingBarberAkkaActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new SleepingBarberAkkaActorBenchmark)
  }

  private final class SleepingBarberAkkaActorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      SleepingBarberConfig.parseArgs(args)
    }

    def printArgInfo() {
      SleepingBarberConfig.printArgs()
    }

    def runIteration() {

      val idGenerator = new AtomicLong(0)

      val system = AkkaActorState.newActorSystem("SleepingBarber")

      val barber = system.actorOf(Props(new BarberActor()))
      val room = system.actorOf(Props(new WaitingRoomActor(SleepingBarberConfig.W, barber)))
      val factoryActor = system.actorOf(Props(new CustomerFactoryActor(idGenerator, SleepingBarberConfig.N, room)))

      AkkaActorState.startActor(barber)
      AkkaActorState.startActor(room)
      AkkaActorState.startActor(factoryActor)

      factoryActor ! Start.ONLY

      AkkaActorState.awaitTermination(system)

      track("CustomerAttempts", idGenerator.get())
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
    }
  }


  private case class Enter(customer: ActorRef, room: ActorRef)

  private case class Returned(customer: ActorRef)


  private class WaitingRoomActor(capacity: Int, barber: ActorRef) extends AkkaActor[AnyRef] {

    private val waitingCustomers = new ListBuffer[ActorRef]()
    private var barberAsleep = true

    override def process(msg: AnyRef) {
      msg match {
        case message: Enter =>

          val customer = message.customer
          if (waitingCustomers.size == capacity) {

            customer ! Full.ONLY

          } else {

            waitingCustomers.append(customer)
            if (barberAsleep) {

              barberAsleep = false
              self ! Next.ONLY

            } else {

              customer ! Wait.ONLY
            }
          }

        case message: Next =>

          if (waitingCustomers.size > 0) {

            val customer = waitingCustomers.remove(0)
            barber ! new Enter(customer, self)

          } else {

            barber ! Wait.ONLY
            barberAsleep = true

          }

        case message: Exit =>

          barber ! Exit.ONLY
          exit()

      }
    }
  }

  private class BarberActor extends AkkaActor[AnyRef] {

    private val random = new PseudoRandom()

    override def process(msg: AnyRef) {
      msg match {
        case message: Enter =>

          val customer = message.customer
          val room = message.room

          customer ! Start.ONLY
          // println("Barber: Processing customer " + customer)
          SleepingBarberConfig.busyWait(random.nextInt(SleepingBarberConfig.AHR) + 10)
          customer ! Done.ONLY
          room ! Next.ONLY

        case message: Wait =>

        // println("Barber: No customers. Going to have a sleep")

        case message: Exit =>

          exit()

      }
    }
  }

  private class CustomerFactoryActor(idGenerator: AtomicLong, haircuts: Int, room: ActorRef) extends AkkaActor[AnyRef] {

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
            room ! Exit.ONLY
            exit()
          }
      }
    }

    private def sendCustomerToRoom() {
      val customer = context.system.actorOf(Props(new CustomerActor(idGenerator.incrementAndGet(), self)))
      AkkaActorState.startActor(customer)

      sendCustomerToRoom(customer)
    }

    private def sendCustomerToRoom(customer: ActorRef) {
      val enterMessage = new Enter(customer, room)
      room ! enterMessage
    }
  }

  private class CustomerActor(val id: Long, factoryActor: ActorRef) extends AkkaActor[AnyRef] {

    override def process(msg: AnyRef) {
      msg match {
        case message: Full =>

          // println("Customer-" + id + " The waiting room is full. I am leaving.")
          factoryActor ! new Returned(self)

        case message: Wait =>

        // println("Customer-" + id + " I will wait.")

        case message: Start =>

        // println("Customer-" + id + " I am now being served.")

        case message: Done =>

          //  println("Customer-" + id + " I have been served.")
          factoryActor ! Done.ONLY
          exit()
      }
    }
  }

}
