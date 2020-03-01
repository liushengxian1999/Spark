package com.SparkDemo.rdd

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Test12 
{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("hello")
    conf.setMaster("local")
    val sc = new SparkContext(conf)
    val rdd1=sc.parallelize(Seq(2,3,4,5,6))
    
//    val value=rdd1.reduce(_+_)//结果算子,行为算子提交作业
//    println(value)
    
//    val array=rdd1.collect()//把rdd的值都拉到driver.而且以集合方式。
//    println(array)
    
//    val value=rdd1.count()//计算rdd中元素的个数
//    println(value)
    
//    val value=rdd1.first()
//    println(value)
//   val array= rdd1.take(2)
//   println(array.toBuffer)
    
    
    
  }
}