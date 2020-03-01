package com.SparkDemo.rdd

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
//1.RDD是scala下一个集合，是不可更改的集合
//2.RDD用来放置大量数据
//3.RDD是一个逻辑集合，真正的数据是在各个服务器上
object TestRddHello
{
  def main(args: Array[String]): Unit = {
     //spark算子：由两部分组成。1.转换算子，意为着不提交任务，只是执行计划。相当于hadoopMappperClass 2.action算子：要结果算子，提交任务
    val conf = new SparkConf()//spark配置对象
    //conf.setAppName("hello13")//应用名
    //启动两个线程
//    conf.setMaster("local[3]")//设置spark程序在哪运行，3个分区rdd或线程
    //创建一个spark上下文，负责 整体运行1.启动程序。2向master 申请资源. 3.把任务指定到某个服务器运行4.负责运行结果的处理
    val sc = new SparkContext(conf)
    val rddWords = sc.parallelize(Seq("hello tom", "hello jerry", "hello jams", "hello jack", "hello jerry", "hello jams", "hello jack"), 3)
    println(rddWords.partitions.length)
    val rddFlatMap = rddWords.flatMap { x => x.split(" "); }
    val rddMap=rddFlatMap.map { x => (x,1) }
    val rddReduceByKey=rddMap.reduceByKey((a:Int,b:Int)=>a+b)    
    rddReduceByKey.foreach(println)
  }
}