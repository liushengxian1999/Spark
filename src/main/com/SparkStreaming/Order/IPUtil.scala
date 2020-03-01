package com.SparkStreaming.Order

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext

import scala.collection.parallel.CollectionsHaveToParArray


object IPUtil {

  // 设置IP信息规则 （传入一个字符串， 返回（开始IP,结束IP,省份））
  def  SetRules(ssc:StreamingContext, path:String)={

    val sc: SparkContext = ssc.sparkContext
    // 读文件
    val lines: RDD[String] = sc.textFile(path)
    // 获取规则
    val rules: Array[(Long, Long, String)] = lines.map(x => {

      val line = x.split("\\|")
      val start: Long = line(2).toLong
      val end: Long = line(3).toLong
      val province = line(6)
      (start, end, province)
    }).collect()
    //广播出去
    sc.broadcast(rules)
  }




}
