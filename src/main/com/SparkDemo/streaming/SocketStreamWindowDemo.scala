//package com.SparkDemo.streaming
//
//import org.apache.spark.SparkConf
//import org.apache.spark.SparkContext
//import org.apache.spark.streaming.StreamingContext
//import org.apache.spark.streaming.Seconds
//import org.apache.spark.storage.StorageLevel
//import org.apache.log4j.Level
//import org.apache.log4j.Logger
//import org.apache.spark.streaming.Duration
//import org.apache.spark.streaming.Durations
//import com.qr.spark.useraction.loglevel.LoggerLevels
//
//object SocketStreamWindowDemo {
//  def main(args: Array[String]): Unit = {
//    LoggerLevels.setStreamingLogLevels()
//    val conf = new SparkConf()
//    conf.setAppName("streaming")
//    conf.setMaster("local[*]")
//    val sc = new SparkContext(conf)
//    val streamingCtx = new StreamingContext(sc, Seconds(30))
//    streamingCtx.checkpoint("file:///d:/111/111")
//    val streaming = streamingCtx.socketTextStream("192.168.38.101", 9999, StorageLevel.MEMORY_AND_DISK)
//    val wordsArray = streaming.flatMap { x => x.split(" ") }
//    val wordPairOne = wordsArray.map { x => (x, 1) }
//    val wordPairReduce=wordPairOne.reduceByKey(_+_)
//  //  val wordPairReduce = wordPairOne.reduceByKeyAndWindow((a:Int,b:Int)=>a+b, Seconds(20), Seconds(15));
//    wordPairReduce.foreach(f => {
//      f.foreach { x => println(x) }
//    })
//    streamingCtx.start()
//    streamingCtx.awaitTermination()
//
//  }
//}