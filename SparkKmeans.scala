package com.spark.demo

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import breeze.linalg.{Vector, DenseVector, squaredDistance}

/**
 * Spark版Kmeans
 */
object SparkKMeans {

  /**
   * 把逗号分隔的字符串转换成Vector
   * @param line
   * @return Vector<String>
   */
  def parseVector(line: String): Vector[Double] = {

    DenseVector(line.split(',').map(_.toDouble))

  }

  /**
   * 计算样本点距离最近的中心点
   * @param p   ----> 样本集合
   * @param centers  --->中心点集合
   * @return --->中心点下标
   */

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

  def main(args: Array[String]) {

    val beginTime = System.currentTimeMillis()
    if (args.length < 3) {
      System.err.println("USAGE: com.spark.demo.WikipediaKMeans <file> <k> <convergeDist>")
      System.exit(1)
    }

    Logger.getLogger("spark").setLevel(Level.ERROR)

    val sparkConf = new SparkConf().setAppName("SparkKMeans")
    val sc = new SparkContext(sparkConf)

    //开始点个数
    val K = args(1).toInt
    //计算精度
    val convergeDist = args(2).toDouble

    /**
     * 训练数据集
     * file : xxx#1.2,3.2,3.1  ---> map(xxx,vector(1.2,3.2,3.1))
     */
    val data = sc.textFile(args(0)).map(
      t => (t.split("#")(0), parseVector(t.split("#")(1)))).cache()


      /**
       * 开始选取k个点 k=3
       * Array(vector(1.2,3.2,3.1),vector(1.2,3.2,3.1),vector(1.2,3.2,3.1))
       */
      val kPoints = data.takeSample(withReplacement = false, K, 42).map(x => x._2).toArray
      //初始距离
      var tempDist = 1.0

    do {

      //计算距离最近的点 map(xxx,vector(1.2,3.2,3.1))---> map(k,(vector(1.2,3.2,3.1),1))
      val closest = data.map (p => (closestPoint(p._2, kPoints), (p._2, 1)))

      /**
       * 计算k个族
       * map(k,(vector(1.2,3.2,3.1),1))  --> map(k,(Array(vector(1.2,3.2,3.1),vector(1.2,3.2,3.1),vector(1.2,3.2,3.1)),n))
       */
      val pointStats = closest.reduceByKey{
        case ((x1, y1), (x2, y2)) => (x1 + x2, y1 + y2)
      }

      /**
       * 用均值计算每个族的新中心点
       * map(k,(Array(vector(1.2,3.2,3.1),vector(1.2,3.2,3.1),vector(1.2,3.2,3.1)),n)) -> Array(vector(1.2,3.2,3.1),vector(1.2,3.2,3.1))
       */
       val newCentroids = pointStats.map {pair =>
        (pair._1, pair._2._1 * (1.0 / pair._2._2))}.collectAsMap()

      /**
       * 计算新点到旧点的距离，用于判断类聚结束
       */
      tempDist = 0.0
      for (i <- 0 until K) {
        tempDist += squaredDistance(kPoints(i),newCentroids(i))
      }

      /**
       * 更新每个族的中心点
       */
      for (newP <- newCentroids) {
        kPoints(newP._1) = newP._2
      }

     // println("Finished iteration (delta = " + tempDist + ")")

    } while (tempDist > convergeDist)


    val endTime = System.currentTimeMillis()

    //打印中心点
    kPoints.map(println(_))

    println("The Application cost Time "+(endTime - beginTime))

    sc.stop()
    System.exit(0)
  }
}
