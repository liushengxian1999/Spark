//package com.SparkDemo.streaming
//
//import org.apache.spark.SparkConf
//import org.apache.spark.SparkContext
//import org.apache.spark.streaming.StreamingContext
//import org.apache.spark.streaming.Seconds
//import org.apache.spark.storage.StorageLevel
//import org.apache.log4j.Level
//import org.apache.log4j.Logger
//
//object SocketStreamDemo {
//  def main(args: Array[String]): Unit = {
//    Logger.getLogger("com.apache.spark").setLevel(Level.FATAL)
//    val conf = new SparkConf()
//    conf.setAppName("streaming")
//    conf.setMaster("local[2]")
//    val sc = new SparkContext(conf)
//    val streamingCtx = new StreamingContext(sc, Seconds(10))
//    streamingCtx.checkpoint("file:///d:/111/111")
//    val streaming = streamingCtx.socketTextStream("192.168.38.100", 9999, StorageLevel.MEMORY_AND_DISK)
//    val wordsArray = streaming.flatMap { x => x.split(" ") }
//    val wordPairOne = wordsArray.map { x => (x, 1) }
//    val wordPairReduce = wordPairOne.updateStateByKey((a:Seq[Int],b:Option[Int])=>Some(a.sum+b.getOrElse(0)))
//    wordPairReduce.foreach(f => {
//      f.foreach { x => println(x) }
//    })
//    streamingCtx.start()
//    streamingCtx.awaitTermination()
//
//  }
//}