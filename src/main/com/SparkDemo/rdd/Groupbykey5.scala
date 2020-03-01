package com.SparkDemo.rdd

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Test5 {
  def main(args: Array[String]): Unit = {
     val conf=new SparkConf();
    conf.setAppName("union")
    conf.setMaster("local[2]")
    val sc=new SparkContext(conf)
    val rdd1=sc.parallelize(Seq("hello tom","hello marry","hello tom"))
    val rdd2=rdd1.flatMap { x => x.split(" ") }
    val rdd3=rdd2.map { x => (x,1) }
    val rdd4=rdd3.groupByKey()//总全局聚合
    val rdd5=rdd4.map(f=>(f._1,f._2.sum))
    rdd5.foreach(println)
    
  }
}