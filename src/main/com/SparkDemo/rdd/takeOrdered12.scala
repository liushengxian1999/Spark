package com.SparkDemo.rdd

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
case class Boy(age: Int)
object takeOrdered12 {
  implicit val ord = new Ordering[Boy]() {
    def compare(x: Boy, y: Boy) = {
      y.age-x.age 
    }
  }
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("hello")
    conf.setMaster("local")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(Seq(Boy(187), Boy(16), Boy(90), Boy(1), Boy(4)))
    val array = rdd.takeOrdered(3)
    println(array.toBuffer)
    rdd.saveAsTextFile("boy")
  }
}