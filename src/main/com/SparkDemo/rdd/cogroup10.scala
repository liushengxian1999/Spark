package com.SparkDemo.rdd

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object cogroup10 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("cogroup")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(Seq("hello tom", "hello jerry", "tom jams"))
    val rdd3 = rdd1.flatMap { x => x.split(" ") }
    val rdd5 = rdd3.map { x => (x, 1) }
    val rdd2 = sc.parallelize(Seq("hello tom", "hello jerry", "tom jams"))
    val rdd4 = rdd2.flatMap { x => x.split(" ") }
    val rdd6 = rdd4.map { x => (x, 1) }
    //先做本RDD的聚合，再RDD之间聚合
    val rdd7=rdd5.cogroup(rdd6)
    val rdd8= rdd7.map(f=>(f._1,f._2._1.sum+f._2._2.sum))
    rdd8.foreach(println)
  }
}