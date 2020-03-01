package com.MapReduce

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Flow {
//Flow 统计上行流量，下行流量 总流量
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Flow").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("D:\\com.MapReduce\\Flow\\data")

    // 切分
    val arr: RDD[Array[String]] = lines.map(_.split("\t"))

    //映射成(电话,(up,down）)
    val map = arr.map(x => (x(1), (x(x.length - 3).toInt, x(x.length - 2).toInt)))
    // 聚合
    val result: RDD[(String, (Int, Int))] = map.reduceByKey((x, y) => {

      (x._1 + y._1, y._2 + x._2)
    })

    result.foreach(println)


//
//    //按照电话分组
//    val group: RDD[(String, Iterable[(String, Int, Int)])] = map.groupBy(_._1)
//    // 值转成元组
//    val change: RDD[(String, (Int, Int))] = group.mapValues(x => (x.toList(0)._2,x.toList(0)._3))
//    // 聚合
//    val sum: RDD[(String, (Int, Int))] = change.reduceByKey((x, y) => {
//      (x._1 + y._1, x._2 + y._2)
//    })
//
//    val result: RDD[(String, (Int, Int, Int))] = sum.map(x => (x._1,(x._2._1, x._2._2, x._2._1+ x._2._2)))
//    result.foreach(println)

  }


}
