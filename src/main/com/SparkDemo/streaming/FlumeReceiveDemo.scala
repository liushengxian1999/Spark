//package com.SparkDemo.streaming
//
//import org.apache.spark.SparkConf
//import org.apache.spark.SparkContext
//import org.apache.spark.streaming.StreamingContext
//import org.apache.spark.streaming.Seconds
//import org.apache.spark.streaming.flume.FlumeUtils
//import com.qr.spark.useraction.loglevel.LoggerLevels
//
//object FlumeReceiveDemo {
//  def main(args: Array[String]): Unit = {
//    LoggerLevels.setStreamingLogLevels()
//    val conf=new SparkConf()
//    conf.setMaster("local[2]")
//    conf.setAppName("hello")
//
//    val sc=new SparkContext(conf)
//    val ssc=new StreamingContext(sc,Seconds(5))
//
//    val streaming=FlumeUtils.createStream(ssc, "192.168.38.1", 9999)
//val str=streaming.repartition(2)
//    val rdd1=str.map { x => (new String(x.event.body.array()),1)}
//    rdd1.foreachRDD(rdd=>{
//      println(rdd.partitions.length)
//    })
//    ssc.start()
//    ssc.awaitTermination()
//  }
//}