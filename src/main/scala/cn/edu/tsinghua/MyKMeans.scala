package cn.edu.tsinghua

import breeze.linalg.{DenseVector, Vector, squaredDistance}
import org.apache.spark._



object MyKMeans {
  val hadoopUrl = "hdfs://hadoop.node.lab.mu:9000"

  def parseVector(line: String): (Int, Int, Vector[Double]) = {
    val lineArr = line.split(' ')
    val length = lineArr.length
    //( fileIndex, regionIndex, featureVector)
    (lineArr(0).toInt, lineArr(length-1).toInt, DenseVector(lineArr.slice(1, length - 1).map(_.toDouble)))
  }

  def closetToString(c: (Int, ((Int, Vector[Double]), Int))): String = {
    var s = ""
    c._2._1._2.foreach { x =>
      s += x
      s += " "
    }
    s += c._1
    s += "\n"
    s
  }

  // (fileIndex, regionIndex, feature vector)
  def closestPoint(p: (Int, Int, Vector[Double]), centers: Array[(Int, Int, Vector[Double])]): Int = {
    var bestIndex = 0
    var closest = Double.PositiveInfinity

    for (i <- 0 until centers.length) {
      val tempDist = squaredDistance(p._3, centers(i)._3)
      if (tempDist < closest) {
        closest = tempDist
        bestIndex = i
      }
    }

    bestIndex
  }

// argument: <imageCount> <number of kmeans clusters> <iteration times> <epsilon distance for stop>
  def main(args: Array[String]): Unit = {
    val imageCount = args(0).toInt
    val numClusters = args(1).toInt
    val numberIterations = args(2).toInt
    val convergeDist = args(3).toDouble

  System.setProperty("HADOOP_USER_NAME", "root")
    val conf = new SparkConf().setAppName("KMeans")
    val files = Array.range(1, imageCount + 1).map(i =>hadoopUrl + "/user/test/" + String.valueOf(i) + ".txt")
    val sc = new SparkContext(conf)
    val data =
      // file structure:Int Double[128] Int: 128-dim sift feature and the feature belongs to which region, first Int is file index
      // Last Int is region index
      sc.textFile(files.mkString(","))
      // data is a RDD set, each element is : (fileIndex, regionIndex, featureVector)
      .map(parseVector).cache()

    // initial centers, random choose from data, which is (fileIndex, regionIndex, feature vector) but region is meaningless
    val kPoints = data.takeSample(withReplacement = false, numClusters, 42)
    var tempDist = 1.0

    var closest = data.map(p => (0, (p, 1)))
    // break if distance from old center and new center is small enough
    while (tempDist > convergeDist) {
      // calculate each point's closes center, closest RDD's element is (index, ( (file, region, feature vector), count )
      // index: belongs to which center, count: used for calculate new centers
      closest = data.map(p => (closestPoint(p, kPoints), (p, 1)))

      // reduce by keys: for item with same index in closest, produce a new map:
      // ((0, 0, feature vector sum), count sum)
      val pointStates = closest.reduceByKey {
        case ((p1, c1), (p2, c2)) => ((0, 0, p1._3 + p2._3), c1 + c2)
      }
      // new k-means center:
      // (RDD_Index, (0, 0, feature_vector_sum / count_sum))
      val newPoints = pointStates.map { pair =>
        (pair._1, (0, 0,pair._2._1._3 * (1.0 / pair._2._2)))
      }.collectAsMap()

      // new centers's distance from old centers
      tempDist = 0.0
      for (i <- 0 until numClusters) {
        tempDist += squaredDistance(kPoints(i)._3, newPoints(i)._3)
      }
      // update center
      for (newP <- newPoints) {
        kPoints(newP._1) = newP._2
      }
      println("Finish iteration(delta = " + tempDist + ")")

    }
    // now we get closest: (k-index, ( (file, region, feature vector), count)
    // for each [file]
    //    for each [region]
    //      get all k-index for this region
    //      generate a {0,1}vector of k-count element: if this [region] has i-index, then v[i]=1 else 0
    //      out put each [region]'s vector as a line in the [file]'s output
    //      no need to output the file's label vector, since we only use it in next step, and the label has nothing to
    //      do with k-means
    println("Final centers: ")
    kPoints.foreach(println)
    // (file index, region index, k-index)
    val pointWithIndex = closest.map(x => "%d %d %d".format(x._2._1._1, x._2._1._2, x._1)).distinct()
    val format = new java.text.SimpleDateFormat("yyyy_MM_dd_hh_mm_ss")
    pointWithIndex.saveAsTextFile("hdfs://hadoop.node.lab.mu:9000/user/temp/" + format.format(new java.util.Date()))
    sc.stop()
  }
}


