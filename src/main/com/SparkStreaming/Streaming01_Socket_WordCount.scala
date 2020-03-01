package com.SparkStreaming

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Milliseconds, Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object Streaming01_Socket_WordCount {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("streaming").setMaster("local[4]")

    // 第二个参数：采集周期
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))

    //从socket端口读数据
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("h1", 8888)

    // 对DStream进行操作 实际上就是对RDD进行操作
    val word: DStream[String] = lines.flatMap(_.split(" "))
    val wordAndOne: DStream[(String, Int)] = word.map((_, 1))
    val result: DStream[(String, Int)] = wordAndOne.reduceByKey(_ + _)

    //输出
    result.print()
    //启动SparkStreaming程序
    ssc.start()
    //Driver等待采集器执行
    ssc.awaitTermination()
  }
}
