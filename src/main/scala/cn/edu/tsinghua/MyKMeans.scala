/**
 * Created by yqh on 2015/5/15.
 */
package  org.apache.spark.example


import java.io.PrintWriter

import breeze.linalg.{Vector, DenseVector, squaredDistance}
import org.apache.spark._


object MyKMeans {
  val numClusters = 2
  val numberIterations = 20
  val pointNumber = 1100
  val convergeDist = 0.01

  def parseVector(line: String): Vector[Double] =
  {
    DenseVector(line.split(' ').map(_.toDouble))
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


  def main(args: Array[String]): Unit =
  {
    var conf = new SparkConf().setAppName("KMeans")
    val sc = new SparkContext(conf)
    val file = sc.textFile("test.txt")
   // val out = new PrintWriter("result.txt")
    val data = file.map(parseVector _).cache()

    val kPoints = data.takeSample(false,numClusters,42).toArray
    var tempDist = 1.0

   // val data = file.map(_.split(' ').map(_.toDouble))

    while(tempDist > convergeDist)
    {
      val closest = data.map(p => (closestPoint(p,kPoints),(p,1)))

      val pointStates = closest.reduceByKey{
        case((p1,c1),(p2,c2)) => (p1+p2, c1+c2)
      }
      val newPoints = pointStates.map {pair =>
        (pair._1, pair._2._1 * (1.0 / pair._2._2))}.collectAsMap()

      tempDist = 0.0
      for(i <-0 until numClusters){
        tempDist += squaredDistance(kPoints(i),newPoints(i))
      }
      for(newP <- newPoints){
        kPoints(newP._1) = newP._2
      }
      println("Finish iteration(delta = "+ tempDist+")")

    }
    val classify = data.map(p => closestPoint(p, kPoints))
    classify.saveAsObjectFile("1.txt")
    println("Final centers: ")
    //kPoints.foreach(println)


    //out.close()
    sc.stop()















  }



/*
  def closestPoint(point:Vector, centers: Array[Vector]) : Int = {
    var bestIndex = 0
    var closest = Double.PositiveInfinity

    for(i <- 0 until centers.length()){
      val tempDist = Vectors.sqdist(point, centers(i));
      if(tempDist < closest){
        closest = tempDist
        bestIndex = i
      }

    }
  }

*/
}

