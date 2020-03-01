package com.SparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Streaming03_Kafka_Window_WordCount {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("streaming").setMaster("local[4]")

    // 第二个参数：采集周期
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))

    // 设置checkPoint
    ssc.checkpoint("cp")

    var groupId = "wordcount"
    var zkQuorum:String = "h1:2181"
    var topicName = "word"
    var topics:Map[String,Int] = Map(topicName->3)
    val ds: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc, zkQuorum, groupId, topics)

    // 窗口大小为采集周期的整数倍，窗口滑动的步长也为采集周期的整数倍
    val window: DStream[(String, String)] = ds.window(Seconds(9), Seconds(3))

    // 对DStream进行操作 实际上就是对RDD进行操作
    val word: DStream[String] = window.flatMap(t => t._2.split(" "))
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
