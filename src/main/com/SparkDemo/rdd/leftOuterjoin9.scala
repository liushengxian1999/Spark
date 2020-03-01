package com.SparkDemo.rdd

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Test9 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("aggregateByKey")
    val sc = new SparkContext(conf)
    //黑名单过滤
    val rdd1 = sc.parallelize(Seq("1,张三", "2,李四", "3,王五"))
    val rdd2 = rdd1.map { x => { val values = x.split(","); (values(0), true) } }
    val rdd3 = sc.textFile("file:///d:/111/black.txt")
    val rdd4 = rdd3.map { x => { val values = x.split("\t"); (values(0), true) } }
    //    rdd4.foreach(println)
    //    val rdd5=rdd2.join(rdd4)//join是内连接
    val rdd5 = rdd2.leftOuterJoin(rdd4)
    val rdd6 = rdd5.filter(f => f._2._2 == None)
    rdd6.foreach(println)
    

  }
}