package edu.rice.habanero.benchmarks.apsp

import edu.rice.habanero.benchmarks.PseudoRandom

/**
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object ApspUtils {

  var graphData: Array[Array[Long]] = null

  def generateGraph() {

    val N = ApspConfig.N
    val W = ApspConfig.W

    val random = new PseudoRandom(N)
    val localData = Array.tabulate[Long](N, N)((i, j) => 0)

    for (i <- 0 until N) {
      for (j <- (i + 1) until N) {
        val r = random.nextInt(W) + 1
        localData(i)(j) = r
        localData(j)(i) = r
      }
    }

    graphData = localData
  }

  def getBlock(srcData: Array[Array[Long]], myBlockId: Int): Array[Array[Long]] = {

    val N = ApspConfig.N
    val B = ApspConfig.B

    val localData = Array.tabulate[Long](B, B)((i, j) => 0)

    val numBlocksPerDim = N / B
    val globalStartRow = (myBlockId / numBlocksPerDim) * B
    val globalStartCol = (myBlockId % numBlocksPerDim) * B

    for (i <- 0 until B) {
      for (j <- 0 until B) {
        localData(i)(j) = srcData(i + globalStartRow)(j + globalStartCol)
      }
    }

    localData
  }

  def print(array: Array[Array[Int]]): Unit = {
    println(array.map(_.mkString(" ")).mkString("\n"))
  }

  def copy(
            srcBlock: Array[Array[Int]],
            destArray: Array[Array[Int]],
            offset: Tuple2[Int, Int],
            blockSize: Int): Unit = {

    for (i <- 0 until blockSize) {
      for (j <- 0 until blockSize) {
        destArray(i + offset._1)(j + offset._2) = srcBlock(i)(j)
      }
    }
  }


}
