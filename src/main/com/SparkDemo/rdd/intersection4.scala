package com.SparkDemo.rdd

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object intersection {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf();
    conf.setAppName("union")
    conf.setMaster("local[2]")
    val sc=new SparkContext(conf)
    val rdd1=sc.parallelize(Seq(1,2,3,4,5,6))
    val rdd2=sc.parallelize(Seq(8,9,0,5,6))
    val rdd4=sc.parallelize(Seq(23,4,4,4,5,6,7))
    val rdd5=rdd4.distinct()
    val rdd3=rdd1.intersection(rdd2)
    rdd3.foreach { x => println(x) }
    rdd5.foreach { x => println(x) }
  }
}