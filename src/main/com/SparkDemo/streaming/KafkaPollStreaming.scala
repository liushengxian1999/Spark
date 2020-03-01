//package com.SparkDemo.streaming
//
//import org.apache.spark.SparkConf
//import org.apache.spark.SparkContext
//import org.apache.spark.streaming.StreamingContext
//import org.apache.spark.streaming.Seconds
//import org.apache.spark.streaming.kafka.KafkaUtils
//import kafka.serializer.StringDecoder
//
//
//object KafkaPollStreaming {
//  def main(args: Array[String]): Unit = {
//    val conf=new SparkConf()
//    conf.setAppName("conf")
//    conf.setMaster("local[2]")
//    val sc=new SparkContext(conf)
//
//    val ssc=new StreamingContext(sc,Seconds(5))
//    ssc.checkpoint("file:///d:/111/data")
//    val kafkaParams=Map("metadata.broker.list"->"192.168.38.100:9092,192.168.38.101:9092,192.168.38.102:9092")
//    val stream=KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc, kafkaParams, Set("info"))
//    val wordDStream=stream.flatMap(f=>f._2.split(" "))
//    val wordKV=wordDStream.map { x => (x,1) }
//    val wordReduceByKey=wordKV.updateStateByKey((x:Seq[Int],y:Option[Int])=>Some(x.sum+y.getOrElse(0)))
//    wordReduceByKey.foreachRDD(rdd=>{
//      rdd.foreachPartition(ff=>{
//        while(ff.hasNext)
//        {
//          //操作数据库
//         println(ff.next())
//        }
//      })
//    })
//    ssc.start()
//    ssc.awaitTermination()
//  }
//}