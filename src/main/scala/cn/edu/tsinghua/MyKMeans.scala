package cn.edu.tsinghua

import breeze.linalg.{DenseVector, Vector, squaredDistance}
import org.apache.spark._



object MyKMeans {
  val numClusters = 2
  val numberIterations = 20
  val pointNumber = 1100
  val convergeDist = 0.01

  def parseVector(line: String): Vector[Double] = {
    DenseVector(line.split(' ').map(_.toDouble))
  }

  def closetToString(c: (Int, (Vector[Double], Int))): String = {
    var s = ""
    c._2._1.foreach { x =>
      s += x
      s += " "
    }
    s += c._1
    s += "\n"
    s
  }

  def closestPoint(p: Vector[Double], centers: Array[Vector[Double]]): Int = {
    var bestIndex = 0
    var closest = Double.PositiveInfinity

    for (i <- 0 until centers.length) {
      val tempDist = squaredDistance(p, centers(i))
      if (tempDist < closest) {
        closest = tempDist
        bestIndex = i
      }
    }

    bestIndex
  }


  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")
    val conf = new SparkConf().setAppName("KMeans").setMaster("local")
    val sc = new SparkContext(conf)
    val file = sc.textFile("hdfs://hadoop.node.lab.mu:9000/user/old/test.txt")
    // val out = new PrintWriter("result.txt")
    val data = file.map(parseVector).cache()

    val kPoints = data.takeSample(withReplacement = false, numClusters, 42)
    var tempDist = 1.0

    var closest = data.map(p => (0, (p, 1)))
    // val data = file.map(_.split(' ').map(_.toDouble))
    while (tempDist > convergeDist) {
      closest = data.map(p => (closestPoint(p, kPoints), (p, 1)))

      val pointStates = closest.reduceByKey {
        case ((p1, c1), (p2, c2)) => (p1 + p2, c1 + c2)
      }
      val newPoints = pointStates.map { pair =>
        (pair._1, pair._2._1 * (1.0 / pair._2._2))
      }.collectAsMap()

      tempDist = 0.0
      for (i <- 0 until numClusters) {
        tempDist += squaredDistance(kPoints(i), newPoints(i))
      }
      for (newP <- newPoints) {
        kPoints(newP._1) = newP._2
      }
      println("Finish iteration(delta = " + tempDist + ")")

    }
    println("Final centers: ")
    kPoints.foreach(println)
    val pointWithIndex = closest.map(closetToString)
    pointWithIndex.saveAsTextFile("hdfs://hadoop.node.lab.mu:9000/user/old/result.txt")
    sc.stop()
  }
}


