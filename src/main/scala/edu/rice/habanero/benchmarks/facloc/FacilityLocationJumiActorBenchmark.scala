package edu.rice.habanero.benchmarks.facloc

import java.util.function.Consumer

import edu.rice.habanero.actors.{JumiActor, JumiActorState, JumiPool}
import edu.rice.habanero.benchmarks.facloc.FacilityLocationConfig.{Box, Point, Position}
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}

/**
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object FacilityLocationJumiActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new FacilityLocationJumiActorBenchmark)
  }

  private final class FacilityLocationJumiActorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      FacilityLocationConfig.parseArgs(args)
    }

    def printArgInfo() {
      FacilityLocationConfig.printArgs()
    }

    def runIteration() {

      val threshold = FacilityLocationConfig.ALPHA * FacilityLocationConfig.F
      val boundingBox = new Box(0, 0, FacilityLocationConfig.GRID_SIZE, FacilityLocationConfig.GRID_SIZE)

      val rootQuadrant = new QuadrantActor(
        null, Position.ROOT, boundingBox, threshold, 0,
        new java.util.ArrayList[Point](), 1, -1, new java.util.ArrayList[Point]())
      rootQuadrant.start()

      val producer = new ProducerActor(rootQuadrant)
      producer.start()

      JumiActorState.awaitTermination()
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double): Unit = {
      if (lastIteration) {
        JumiPool.shutdown()
      }
    }
  }


  private abstract class Msg()

  private case class FacilityMsg(positionRelativeToParent: Int, depth: Int, point: Point, fromChild: Boolean) extends Msg

  private case class NextCustomerMsg() extends Msg

  private case class CustomerMsg(producer: JumiActor[AnyRef], point: Point) extends Msg

  private case class RequestExitMsg() extends Msg

  private case class ConfirmExitMsg(facilities: Int, supportCustomers: Int) extends Msg


  private class ProducerActor(consumer: JumiActor[AnyRef]) extends JumiActor[AnyRef] {

    private val selfActor = this
    private var itemsProduced = 0

    override def onPostStart(): Unit = {
      produceCustomer()
    }

    private def produceCustomer(): Unit = {
      consumer.send(CustomerMsg(selfActor, Point.random(FacilityLocationConfig.GRID_SIZE)))
      itemsProduced += 1
    }

    override def process(message: AnyRef) {
      message match {
        case msg: NextCustomerMsg =>
          if (itemsProduced < FacilityLocationConfig.NUM_POINTS) {
            produceCustomer()
          } else {
            consumer.send(RequestExitMsg())
            exit()
          }
      }
    }
  }

  private class QuadrantActor(parent: QuadrantActor,
                              positionRelativeToParent: Int,
                              val boundary: Box,
                              threshold: Double,
                              depth: Int,
                              initLocalFacilities: java.util.List[Point],
                              initKnownFacilities: Int,
                              initMaxDepthOfKnownOpenFacility: Int,
                              initCustomers: java.util.List[Point]) extends JumiActor[AnyRef] {

    private val selfActor = this
    // the facility associated with this quadrant if it were to open
    private val facility: Point = boundary.midPoint()

    // all the local facilities from corner ancestors
    val localFacilities = new java.util.ArrayList[Point]()
    localFacilities.addAll(initLocalFacilities)
    localFacilities.add(facility)

    private var knownFacilities = initKnownFacilities
    private var maxDepthOfKnownOpenFacility = initMaxDepthOfKnownOpenFacility
    private var terminatedChildCount = 0

    // the support customers for this Quadrant
    private val supportCustomers = new java.util.ArrayList[Point]()

    private var childrenFacilities = 0
    private var facilityCustomers = 0

    // null when closed, non-null when open
    private var children: List[QuadrantActor] = null
    private var childrenBoundaries: List[Box] = null

    // the cost so far
    private var totalCost = 0.0

    initCustomers.forEach(new Consumer[Point] {
      override def accept(loopPoint: Point): Unit = {
        if (boundary.contains(loopPoint)) {
          addCustomer(loopPoint)
        }
      }

      override def andThen(after: Consumer[_ >: Point]): Consumer[Point] = {
        this
      }
    })

    override def process(msg: AnyRef) {
      msg match {
        case customer: CustomerMsg =>

          val point: Point = customer.point
          if (children == null) {

            // no open facility
            addCustomer(point)
            if (totalCost > threshold) {
              partition()
            }

          } else {

            // a facility is already open, propagate customer to correct child
            var index = 0
            while (index <= 4) {
              val loopChildBoundary = childrenBoundaries(index)
              if (loopChildBoundary.contains(point)) {
                children(index).send(customer)
                index = 5
              } else {
                index += 1
              }
            }
          }

          if (parent eq null) {
            // request next customer
            customer.producer.send(NextCustomerMsg())
          }

        case facility: FacilityMsg =>

          val point = facility.point
          val fromChild = facility.fromChild

          knownFacilities += 1
          localFacilities.add(point)

          if (fromChild) {
            notifyParentOfFacility(point, facility.depth)
            if (facility.depth > maxDepthOfKnownOpenFacility) {
              maxDepthOfKnownOpenFacility = facility.depth
            }

            // notify sibling
            val childPos = facility.positionRelativeToParent
            val siblingPos: Int = if (childPos == Position.TOP_LEFT) {
              Position.BOT_RIGHT
            } else if (childPos == Position.TOP_RIGHT) {
              Position.BOT_LEFT
            } else if (childPos == Position.BOT_RIGHT) {
              Position.TOP_LEFT
            } else {
              Position.TOP_RIGHT
            }
            children(siblingPos).send(FacilityMsg(Position.UNKNOWN, depth, point, false))

          } else {

            // notify all children
            if (children ne null) {
              children.foreach {
                loopChild =>
                  loopChild.send(FacilityMsg(Position.UNKNOWN, depth, point, false))
              }
            }
          }

        case exitMsg: RequestExitMsg =>

          if (children ne null) {
            children.foreach {
              loopChild =>
                loopChild.send(exitMsg)
            }
          } else {
            // No children, notify parent and safely exit
            safelyExit()
          }

        case exitMsg: ConfirmExitMsg =>

          // child has sent a confirmation that it has exited
          terminatedChildCount += 1

          childrenFacilities += exitMsg.facilities
          facilityCustomers += exitMsg.supportCustomers

          if (terminatedChildCount == 4) {
            // all children terminated
            safelyExit()
          }
      }
    }

    private def addCustomer(point: Point): Unit = {
      supportCustomers.add(point)
      val minCost = findCost(point)
      totalCost += minCost
    }

    private def findCost(point: Point): Double = {
      var result = Double.MaxValue

      // there will be at least one facility
      localFacilities.forEach(new Consumer[Point] {
        override def accept(loopPoint: Point): Unit = {
          val distance = loopPoint.getDistance(point)
          if (distance < result) {
            result = distance
          }
        }

        override def andThen(after: Consumer[_ >: Point]): Consumer[Point] = {
          this
        }
      })

      result
    }

    private def notifyParentOfFacility(p: Point, depth: Int): Unit = {
      //println("Quadrant-" + id + ": notifyParentOfFacility: parent = " + parent)
      if (parent ne null) {
        //println("Quadrant-" + id + ": notifyParentOfFacility: sending msg to parent: " + parent.id)
        parent.send(FacilityMsg(positionRelativeToParent, depth, p, true))
      }
    }

    private def partition(): Unit = {

      // notify parent that opened a new facility
      notifyParentOfFacility(facility, depth)
      maxDepthOfKnownOpenFacility = math.max(maxDepthOfKnownOpenFacility, depth)

      // create children and propagate their share of customers to them
      val firstBoundary: Box = new Box(boundary.x1, facility.y, facility.x, boundary.y2)
      val secondBoundary: Box = new Box(facility.x, facility.y, boundary.x2, boundary.y2)
      val thirdBoundary: Box = new Box(boundary.x1, boundary.y1, facility.x, facility.y)
      val fourthBoundary: Box = new Box(facility.x, boundary.y1, boundary.x2, facility.y)

      val firstChild = new QuadrantActor(
        selfActor, Position.TOP_LEFT, firstBoundary, threshold, depth + 1,
        localFacilities, knownFacilities, maxDepthOfKnownOpenFacility, supportCustomers)
      firstChild.start()

      val secondChild = new QuadrantActor(
        selfActor, Position.TOP_RIGHT, secondBoundary, threshold, depth + 1,
        localFacilities, knownFacilities, maxDepthOfKnownOpenFacility, supportCustomers)
      secondChild.start()

      val thirdChild = new QuadrantActor(
        selfActor, Position.BOT_LEFT, thirdBoundary, threshold, depth + 1,
        localFacilities, knownFacilities, maxDepthOfKnownOpenFacility, supportCustomers)
      thirdChild.start()

      val fourthChild = new QuadrantActor(
        selfActor, Position.BOT_RIGHT, fourthBoundary, threshold, depth + 1,
        localFacilities, knownFacilities, maxDepthOfKnownOpenFacility, supportCustomers)
      fourthChild.start()


      children = List[QuadrantActor](firstChild, secondChild, thirdChild, fourthChild)
      childrenBoundaries = List[Box](firstBoundary, secondBoundary, thirdBoundary, fourthBoundary)

      // support customers have been distributed to the children
      supportCustomers.clear()
    }

    private def safelyExit(): Unit = {

      if (parent ne null) {
        val numFacilities = if (children ne null) childrenFacilities + 1 else childrenFacilities
        val numCustomers = facilityCustomers + supportCustomers.size
        parent.send(ConfirmExitMsg(numFacilities, numCustomers))
      } else {
        val numFacilities = childrenFacilities + 1
        println("  Num Facilities: " + numFacilities + ", Num customers: " + facilityCustomers)
      }
      exit()

    }
  }

}
