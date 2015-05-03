package com.kmeans.local

import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.util.Random

/**
 *
 * Scala实现是K-Means算法
 */
object LocalKmeans {

  /**
   * 把逗号分隔的字符串转换成Vector
   * @param line
   * @return
   */
  def parseVector(line : String):Vector[Double] = {

         line.split(',').map(_.toDouble).toVector

  }

  /**
   * 计算样本点最近的中心点下标
   * @param p
   * @param Points
   * @return
   */
  def closestPoint(p : Vector[Double],Points : Array[Vector[Double]]):Int = {

       var bestIndex = 0
       var closest = Double.MaxValue
       for( i <- 0 until Points.length ){
         val tempDist = getDist(p,Points(i))
         if(tempDist < closest){
           closest = tempDist
           bestIndex = i

         }

       }

    bestIndex

  }

  /**
   * 计算两点相加
   * @param p1 : Vector[Double]
   * @param p2 : Vector[Double]
   * @return p : Vector[Double]
   */
  def addPoints(p1:Vector[Double],p2:Vector[Double]):Vector[Double] = {

    //样本点的维度
    val demin = p1.length

    val newVector = new Array[Double](demin)

    for (i <- 0 until demin){

      newVector(i) = p1(i) + p2(i)

    }

    newVector.toVector

  }

  /**
   * 计算一个簇的平均值点
   * @param cluster : Array[Vector[Double]]
   * @return p : Vector[Double]
   */
  def getMeansPoint(cluster : Array[Vector[Double]]):Vector[Double] ={

          val sizeOfCluster = cluster.length

          // 计算族的总和 （x+y)
          val newPoint = cluster.reduceLeft((x,y)=>addPoints(x,y))
         //每个维度除以总数


         newPoint.map(_/sizeOfCluster)

  }

  /**
   * 计算两点之间的欧式距离
   * 点的维度相同
   * @param p1 : Vector[Double]
   * @param p2 : Vector[Double]
   * @return dist : Double
   */
  def getDist(p1 : Vector[Double],p2 : Vector[Double]):Double={

    val demins = p1.length

    var tmp : Double = 0.00

    for(i <- 0 until demins){

       tmp = tmp + (p1(i)-p2(i))*(p1(i)-p2(i))

    }
    Math.sqrt(tmp)

  }



   def main (args: Array[String]) {
     val beginTime = System.currentTimeMillis()
     if (args.length < 3 ) {
       System.err.println("USAGE: Scala com.kmeans.local.LocalKmeans  <trainfile> <k> <convergeDist> ")
       System.exit(1)
     }

     val trainFilePath = args(0)
     val k = args(1).toInt
     val convergeDist = args(2).toDouble


     val points = new ArrayBuffer[Vector[Double]]

     //读取训练数据文件
     Source.fromFile(trainFilePath).getLines().foreach(
       line => {
         points += parseVector(line.split("#")(1))
       }
       )

     //样本点总数
     val pointsSize = points.length
     //k个中心点
     val k_points  = new Array[Vector[Double]](k)
     //初始化K个中心点
     for( i <- 0 until k){
       //可能选取的点会重复
      val index = new Random().nextInt(pointsSize)
       k_points(i) = points(index)
     }


     //每一次迭代计算的中心点之间的距离
     var tempDist = 0.00
     //迭代计算
     do{

       //初始化新旧中心点之间的距离
       tempDist = 0.00

       //最近质心为k,将数据集分簇
       val closest = points.groupBy(closestPoint(_,k_points))

       // 分别计算簇中数据集的平均数，得到每个簇的新中心点
      val newCenterPoint =  new Array[Vector[Double]](k)

       //计算新的中心点
       for(i <- 0 until k){

         newCenterPoint(i) =getMeansPoint(closest.get(i).get.toArray)

       }
       //计算精度是否达到要求
       //计算旧中心点到新中心点之间的距离之和
       for(i <- 0 until k){

         tempDist += getDist(k_points(i),newCenterPoint(i))

       }

       //更新中心点
       for(i <- 0 until k){

        k_points(i) = newCenterPoint(i)

       }

     }while(tempDist>convergeDist)

     val endTime = System.currentTimeMillis();

     //打印中心点
     println("中心点")
     k_points.map(println(_))
     println("The Application Cost:", endTime-beginTime);

   }

  
}
