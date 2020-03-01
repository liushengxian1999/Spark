package com.SparkDemo.rdd

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Test3 {
  def main(args: Array[String]): Unit = 
  {
    
    val conf=new SparkConf();
    conf.setAppName("union")
    conf.setMaster("local[2]")
    val sc=new SparkContext(conf)
    //创建RDD两种方式 1.通过并行化方法 2.通过 sc.textFile()
   val rdd1=sc.parallelize(Seq("1,zhangsan,nan","2,lisi,nan"))
    val rdd2=sc.parallelize(Seq("3,wangwu,nan","4,liuneng,nan"))
   val rdd3= rdd1.union(rdd2)//通过一种方式计算一个rdd 通过另外一种又计算一个rdd
   rdd3.foreach { x =>println(x) }
    
  }
}