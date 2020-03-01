package com.SparkDemo.rdd

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object sortByKey7 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("sortByKey")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(Seq("hello tom", "hello jerry", "hello jerry", "hello jerry", "hello jerry", "tom go","aa bb","cc dd"), 2)
    val rdd2 = rdd1.flatMap { x => x.split(" ") }
    val rdd3 = rdd2.map { x => (x, 1) }
    val rdd4=rdd3.reduceByKey(_+_)
    val rdd5= rdd4.sortByKey(true)//true升序 false降序
    rdd5.foreach(println)
  }
}