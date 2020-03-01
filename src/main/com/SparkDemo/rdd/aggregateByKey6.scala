package com.SparkDemo.rdd

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object aggregateByKey6 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("aggregateByKey")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(Seq("hello tom", "hello jerry", "hello jerry","hello jerry","hello jerry","tom go"),1)
    val rdd2 = rdd1.flatMap { x => x.split(" ") }
    val rdd3 = rdd2.map { x => (x, 1) }
    
    //Map
    val rdd4=rdd3.aggregateByKey(0)((a,b)=>{println(a+" =============="+b);a+b}, (c,d)=>{println(c+"  "+d);c+d})
    rdd4.foreach(println)
  }
}