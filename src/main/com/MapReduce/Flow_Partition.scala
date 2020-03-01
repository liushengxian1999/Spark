package com.MapReduce

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable

object Flow_Partition {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Flow").setMaster("local")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("D:\\com.MapReduce\\Flow\\data")

    // 转换成（电话，(up，down)）
    val change = lines.map(x => {
      val arr = x.split("\t")
      (arr(1), (arr(arr.length - 3).toInt, arr(arr.length - 2).toInt))
    })

    // 获得phone数组
    val phone = lines.map(_.split("\t")(1)).distinct().collect()
    // 构造分区器
    val mypartition = new MyPartition(phone)

    //按照电话 分区

    val result: RDD[(String, (Int, Int))] = change.reduceByKey(mypartition,(x, y) => {

      (x._1 + y._1, x._2 + y._2)

    })
    result.saveAsTextFile("d:/result")


  }
}

class MyPartition(arr:Array[String]) extends Partitioner{

  private val hash = new mutable.HashMap[String,Int]()

  hash("135")=0
  hash("136")=1
  hash("137")=2
  hash("138")=3
  hash("139")=4

  override def numPartitions: Int = 6

  override def getPartition(key: Any): Int = {

    val k = key.asInstanceOf[String].substring(0,3)

    if(hash.contains(k)) hash(k) else 5



  }
}
