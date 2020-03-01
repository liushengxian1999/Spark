//package com.SparkDemo.streaming
//
//import org.apache.spark.SparkConf
//import org.apache.spark.SparkContext
//import org.apache.spark.streaming.StreamingContext
//import org.apache.spark.streaming.Seconds
//import org.apache.spark.streaming.kafka.KafkaUtils
//import org.apache.spark.storage.StorageLevel
//import com.qr.spark.useraction.loglevel.LoggerLevels
//
//object KafkaPushStreamingDemo {
//  def main(args: Array[String]): Unit = {
//     LoggerLevels.setStreamingLogLevels()
//    val conf = new SparkConf()
//    conf.setAppName("kafka")
//    conf.setMaster("local[2]")
//    val sc = new SparkContext(conf)
//    val ssc = new StreamingContext(sc, Seconds(5))
//    val map = Map("t1" -> 1)
//
//    val stream = KafkaUtils.createStream(ssc, "192.168.38.101:2181,192.168.38.102:2181,192.168.38.103:2181", "t1", map, StorageLevel.MEMORY_AND_DISK)
//    val rddMap = stream.map(f => f._2)
//    rddMap.foreachRDD(f=>{
//     println(f.partitions.length+"================")
//    })
//    ssc.start()
//    ssc.awaitTermination()
//
//  }
//}