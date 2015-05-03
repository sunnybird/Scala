
package com.spark.data
import scala.util.Random
import java.io.{PrintWriter, File}

/**
 * Created by hadoop on 15-4-26.
 */
object KmeansData {
  //数据属性20维
  val demin : Int = 20
  //一行数据size
  val rowsize : Long = 60

  val dir = "/home/hadoop/spark-1.2.0-bin-hadoop2.4/Data/"

  /**
   * 根据不同随机数范围生成一个20纬的向量
   * @param min
   * @param max
   * @return
   */
  def generateVector(min:Int,max:Int):Array[Int]={
    val range = max - min
    val demainVector = new Array[Int](demin)
    val random = new Random()
    for( i <- 0 until demin){
      demainVector(i) = (random.nextInt(range)+min)
    }
    demainVector
  }

  /**
   *生成不同类别的样本数据
   * @return
   */
  def generateData():Array[Int]={
    val random = new Random()
    val _type : Int = random.nextInt(3)
    _type match {
      case 0 => generateVector(0,20)
      case 1 => generateVector(40,60)
      case 2 => generateVector(80,100)
    }
  }

  /**
   * 把向量数据写入文件 格式：A#1,2,3,4,....
   * @param _path
   * @param _size
   */
  def writeVector2File(_path:String,_size : Long = 0) : Unit = {

    val filelength = (_size/rowsize).toInt
    val writer = new PrintWriter(new File(dir+_path))
    for(i <- 0 to filelength){
      val colValue = generateData()
      val line = new StringBuffer

      if(colValue(0)<21)
        line.append("A#")
      else if(colValue(0)<61)
        line.append("B#")
      else
        line.append("C#")

      for( j <- 0 until demin ){

        if(j == demin-1)
          line.append(colValue(j))
        else
          line.append(colValue(j)+",")
      }
      writer.write(line.toString.trim)
      writer.write("\r\n")
      writer.flush()
    }
    writer.close()
  }


  
  def main (args: Array[String]) {

    if(args.length<2){
      System.err.println("USAGE: Scala com.spark.data.KmeansData  fileSize(MB) filepath ")
      System.exit(1)
    }
    val filesize = args(0).toInt
    val filepath = args(1).toString
    
    println("Genereate"+filesize+"Mb Data to "+filepath)
    writeVector2File(filepath+"data"+filesize+".txt",filesize*1024*1024)
    println("finished..................")

  }
}
