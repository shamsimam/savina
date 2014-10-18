package edu.rice.habanero.benchmarks.uct

import java.util.Random

import edu.rice.habanero.actors.{JetlangActor, JetlangActorState, JetlangPool}
import edu.rice.habanero.benchmarks.uct.UctConfig._
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}

/**
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object UctJetlangActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new UctJetlangActorBenchmark)
  }

  private final class UctJetlangActorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      UctConfig.parseArgs(args)
    }

    def printArgInfo() {
      UctConfig.printArgs()
    }

    def runIteration() {
      val rootActor = new RootActor()
      rootActor.start()
      rootActor.send(GenerateTreeMessage.ONLY)

      JetlangActorState.awaitTermination()
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
      if (lastIteration) {
        JetlangPool.shutdown()
      }
    }
  }

  /**
   * @author xinghuizhao
   * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
   */
  protected class RootActor extends JetlangActor[AnyRef] {

    private final val ran: Random = new Random(2)
    private var height: Int = 1
    private var size: Int = 1
    private final val children = new Array[JetlangActor[AnyRef]](UctConfig.BINOMIAL_PARAM)
    private final val hasGrantChildren = new Array[Boolean](UctConfig.BINOMIAL_PARAM)
    private var traversed: Boolean = false
    private var finalSizePrinted: Boolean = false

    override def process(theMsg: AnyRef) {
      theMsg match {
        case _: UctConfig.GenerateTreeMessage =>
          generateTree()
        case grantMessage: UctConfig.UpdateGrantMessage =>
          updateGrant(grantMessage.childId)
        case booleanMessage: UctConfig.ShouldGenerateChildrenMessage =>
          val sender: JetlangActor[AnyRef] = booleanMessage.sender.asInstanceOf[JetlangActor[AnyRef]]
          checkGenerateChildrenRequest(sender, booleanMessage.childHeight)
        case _: UctConfig.PrintInfoMessage =>
          printInfo()
        case _: UctConfig.TerminateMessage =>
          terminateMe()
        case _ =>
      }
    }

    /**
     * This message is called externally to create the BINOMIAL_PARAM tree
     */
    def generateTree() {
      height += 1
      val computationSize: Int = getNextNormal(UctConfig.AVG_COMP_SIZE, UctConfig.STDEV_COMP_SIZE)

      var i: Int = 0
      while (i < UctConfig.BINOMIAL_PARAM) {

        hasGrantChildren(i) = false
        children(i) = NodeActor.createNodeActor(this, this, height, size + i, computationSize, urgent = false)

        i += 1
      }
      size += UctConfig.BINOMIAL_PARAM

      var j: Int = 0
      while (j < UctConfig.BINOMIAL_PARAM) {

        children(j).send(TryGenerateChildrenMessage.ONLY)

        j += 1
      }
    }

    /**
     * This message is called by a child node before generating children;
     * the child may generate children only if this message returns true
     *
     * @param childName The child name
     * @param childHeight The height of the child in the tree
     */
    def checkGenerateChildrenRequest(childName: JetlangActor[AnyRef], childHeight: Int) {
      if (size + UctConfig.BINOMIAL_PARAM <= UctConfig.MAX_NODES) {
        val moreChildren: Boolean = ran.nextBoolean
        if (moreChildren) {
          val childComp: Int = getNextNormal(UctConfig.AVG_COMP_SIZE, UctConfig.STDEV_COMP_SIZE)
          val randomInt: Int = ran.nextInt(100)
          if (randomInt > UctConfig.URGENT_NODE_PERCENT) {
            childName.send(new UctConfig.GenerateChildrenMessage(size, childComp))
          } else {
            childName.send(new UctConfig.UrgentGenerateChildrenMessage(ran.nextInt(UctConfig.BINOMIAL_PARAM), size, childComp))
          }
          size += UctConfig.BINOMIAL_PARAM
          if (childHeight + 1 > height) {
            height = childHeight + 1
          }
        }
        else {
          if (childHeight > height) {
            height = childHeight
          }
        }
      }
      else {
        if (!finalSizePrinted) {
          System.out.println("final size= " + size)
          System.out.println("final height= " + height)
          finalSizePrinted = true
        }
        if (!traversed) {
          traversed = true
          traverse()
        }
        terminateMe()
      }
    }

    /**
     * This method is called by getBoolean in order to generate computation times for actors, which
     * follows a normal distribution with mean value and a std value
     */
    def getNextNormal(pMean: Int, pDev: Int): Int = {
      var result: Int = 0
      while (result <= 0) {
        val tempDouble: Double = ran.nextGaussian * pDev + pMean
        result = Math.round(tempDouble).asInstanceOf[Int]
      }
      result
    }

    /**
     * This message is called by a child node to indicate that it has children
     */
    def updateGrant(childId: Int) {
      hasGrantChildren(childId) = true
    }

    /**
     * This is the method for traversing the tree
     */
    def traverse() {
      var i: Int = 0
      while (i < UctConfig.BINOMIAL_PARAM) {
        children(i).send(TraverseMessage.ONLY)
        i += 1
      }
    }

    def printInfo() {
      System.out.println("0 0 children starts 1")
      var i: Int = 0
      while (i < UctConfig.BINOMIAL_PARAM) {

        children(i).send(PrintInfoMessage.ONLY)
        i += 1
      }

    }

    def terminateMe() {
      if (hasExited()) {
        return
      }
      var i: Int = 0
      while (i < UctConfig.BINOMIAL_PARAM) {
        children(i).send(TerminateMessage.ONLY)
        i += 1
      }

      exit()
    }
  }

  /**
   * @author xinghuizhao
   * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
   */
  protected object NodeActor {
    def createNodeActor(parent: JetlangActor[AnyRef], root: JetlangActor[AnyRef], height: Int, id: Int, comp: Int, urgent: Boolean): NodeActor = {
      val nodeActor: NodeActor = new NodeActor(parent, root, height, id, comp, urgent)
      nodeActor.start()
      nodeActor
    }

    private final val dummy: Int = 40000
  }

  protected class NodeActor(myParent: JetlangActor[AnyRef], myRoot: JetlangActor[AnyRef], myHeight: Int, myId: Int, myCompSize: Int, isUrgent: Boolean) extends JetlangActor[AnyRef] {

    private var urgentChild: Int = 0
    private var hasChildren: Boolean = false
    private final val children = new Array[JetlangActor[AnyRef]](UctConfig.BINOMIAL_PARAM)
    private final val hasGrantChildren = new Array[Boolean](UctConfig.BINOMIAL_PARAM)

    override def process(theMsg: AnyRef) {
      theMsg match {
        case _: UctConfig.TryGenerateChildrenMessage =>
          tryGenerateChildren()
        case childrenMessage: UctConfig.GenerateChildrenMessage =>
          generateChildren(childrenMessage.currentId, childrenMessage.compSize)
        case childrenMessage: UctConfig.UrgentGenerateChildrenMessage =>
          generateUrgentChildren(childrenMessage.urgentChildId, childrenMessage.currentId, childrenMessage.compSize)
        case grantMessage: UctConfig.UpdateGrantMessage =>
          updateGrant(grantMessage.childId)
        case _: UctConfig.TraverseMessage =>
          traverse()
        case _: UctConfig.UrgentTraverseMessage =>
          urgentTraverse()
        case _: UctConfig.PrintInfoMessage =>
          printInfo()
        case _: UctConfig.GetIdMessage =>
          getId
        case _: UctConfig.TerminateMessage =>
          terminateMe()
        case _ =>
      }
    }

    /**
     * This message is called by parent node, try to generate children of this node.
     * If the "getBoolean" message returns true, the node is allowed to generate BINOMIAL_PARAM children
     */
    def tryGenerateChildren() {
      UctConfig.loop(100, NodeActor.dummy)
      myRoot.send(new UctConfig.ShouldGenerateChildrenMessage(this, myHeight))
    }

    def generateChildren(currentId: Int, compSize: Int) {
      val myArrayId: Int = myId % UctConfig.BINOMIAL_PARAM
      myParent.send(new UctConfig.UpdateGrantMessage(myArrayId))
      val childrenHeight: Int = myHeight + 1
      val idValue: Int = currentId

      var i: Int = 0
      while (i < UctConfig.BINOMIAL_PARAM) {

        children(i) = NodeActor.createNodeActor(this, myRoot, childrenHeight, idValue + i, compSize, urgent = false)
        i += 1
      }

      hasChildren = true

      var j: Int = 0
      while (j < UctConfig.BINOMIAL_PARAM) {

        children(j).send(TryGenerateChildrenMessage.ONLY)
        j += 1
      }
    }

    def generateUrgentChildren(urgentChildId: Int, currentId: Int, compSize: Int) {
      val myArrayId: Int = myId % UctConfig.BINOMIAL_PARAM
      myParent.send(new UctConfig.UpdateGrantMessage(myArrayId))
      val childrenHeight: Int = myHeight + 1
      val idValue: Int = currentId
      urgentChild = urgentChildId

      var i: Int = 0
      while (i < UctConfig.BINOMIAL_PARAM) {

        children(i) = NodeActor.createNodeActor(this, myRoot, childrenHeight, idValue + i, compSize, i == urgentChild)
        i += 1
      }

      hasChildren = true

      var j: Int = 0
      while (j < UctConfig.BINOMIAL_PARAM) {

        children(j).send(TryGenerateChildrenMessage.ONLY)
        j += 1
      }
    }

    /**
     * This message is called by a child node to indicate that it has children
     */
    def updateGrant(childId: Int) {
      hasGrantChildren(childId) = true
    }

    /**
     * This message is called by parent while doing a traverse
     */
    def traverse() {
      UctConfig.loop(myCompSize, NodeActor.dummy)
      if (hasChildren) {

        var i: Int = 0
        while (i < UctConfig.BINOMIAL_PARAM) {

          children(i).send(TraverseMessage.ONLY)
          i += 1
        }
      }
    }

    /**
     * This message is called by parent while doing traverse, if this node is an urgent node
     */
    def urgentTraverse() {
      UctConfig.loop(myCompSize, NodeActor.dummy)
      if (hasChildren) {
        if (urgentChild != -1) {

          var i: Int = 0
          while (i < UctConfig.BINOMIAL_PARAM) {
            if (i != urgentChild) {
              children(i).send(TraverseMessage.ONLY)
            } else {
              children(urgentChild).send(UrgentTraverseMessage.ONLY)
            }
            i += 1
          }
        } else {

          var i: Int = 0
          while (i < UctConfig.BINOMIAL_PARAM) {
            children(i).send(TraverseMessage.ONLY)
            i += 1
          }
        }
      }
      if (isUrgent) {
        System.out.println("urgent traverse node " + myId + " " + System.currentTimeMillis)
      } else {
        System.out.println(myId + " " + System.currentTimeMillis)
      }
    }

    def printInfo() {
      if (isUrgent) {
        System.out.print("Urgent......")
      }
      if (hasChildren) {
        System.out.println(myId + " " + myCompSize + "  children starts ")

        var i: Int = 0
        while (i < UctConfig.BINOMIAL_PARAM) {
          children(i).send(PrintInfoMessage.ONLY)
          i += 1
        }
      } else {
        System.out.println(myId + " " + myCompSize)
      }
    }

    def getId: Int = {
      myId
    }

    def terminateMe() {
      if (hasExited()) {
        return
      }
      if (hasChildren) {

        var i: Int = 0
        while (i < UctConfig.BINOMIAL_PARAM) {
          children(i).send(TerminateMessage.ONLY)
          i += 1
        }
      }
      exit()
    }
  }

}
