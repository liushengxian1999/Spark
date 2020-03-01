

package com.SparkDemo.accumulator

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.Map
object Test120
{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("acculator")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(Seq("hello", "tom", "jerry", "marry", 
        "zhangsan", "lisi", "jams", "tom", "jerry", "marry", "zhangsan",
        "lisi", "jams", "tom", "jerry", "marry", "zhangsan", "lisi", "jams",
        "tom", "jerry", "marry", "zhangsan", "lisi", "jams", "tom", "jerry", "marry", 
        "zhangsan", "lisi", "jams", "tom", "jerry", "marry", "zhangsan", "lisi", "jams",
"tom", "jerry", "marry", "zhangsan", "lisi", "jams", "tom", "jerry", "marry", "zhangsan", 
"lisi", "jams", "tom", "jerry", "marry", "zhangsan", "lisi", "jams"))
    val acc = sc.accumulator(Map[String,Int]())(new WordAcculator())
    val accBroadcast= sc.broadcast(acc)
    val rdd2=rdd1.map { x => { 
      accBroadcast.value.+=(Map(x->1))
      (x, 1) 
      } }
    val count=rdd2.count()
    println(count+"    "+acc.value)
    
    
  }
}