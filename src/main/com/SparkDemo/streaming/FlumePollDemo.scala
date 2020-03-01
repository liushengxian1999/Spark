//package com.SparkDemo.streaming
//
//import org.apache.spark.SparkConf
//import org.apache.spark.SparkContext
//import org.apache.spark.streaming.StreamingContext
//import org.apache.spark.streaming.Seconds
//import java.net.InetSocketAddress
//import org.apache.spark.storage.StorageLevel
//import com.qr.spark.useraction.loglevel.LoggerLevels
//import org.apache.spark.HashPartitioner
//
//object FlumePollDemo {
//  def main(args: Array[String]): Unit =
//  {
//    LoggerLevels.setStreamingLogLevels()
//    val conf = new SparkConf().setAppName("streaming").setMaster("local[2]")
//    val sc = new SparkContext(conf)
//    sc.setCheckpointDir("file:///d:/111/111")
//    val ssc = new StreamingContext(sc, Seconds(5))
//    //new InetSocketAddress("192.168.8.200",8888) 指的是flume本机地址，可以写多个flumeip地址
//    val dstream = FlumeUtils.createPollingStream(ssc, List(new InetSocketAddress("192.168.38.103",9999)), StorageLevel.MEMORY_AND_DISK)
//    val flatMapWord = dstream.flatMap { x => new String(x.event.body.array()).split(" ") }.map { x => (x, 1) };
//
//    val sumWord = flatMapWord.updateStateByKey((x: Iterator[(String, Seq[Int], Option[Int])]) => {
//      x.map { case (x, y, z) => { (x, y.sum + z.getOrElse(0)) } } //case 用大括号
//    }, new HashPartitioner(3), false)
//    sumWord.print()
//    ssc.start()
//    ssc.awaitTermination()
//
//  }
//}