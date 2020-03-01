package com.MapReduce

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Order {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Order").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("D:\\com.MapReduce\\Order\\data",1)

    //(订单编号,(用户编号,商品名称,价格,数量,总价))
    val line: RDD[(String, (String, String, Float, Int, Float))] = lines.map(x => {
      val sp = x.split(",")
      var price = sp(3).toFloat
      var num = sp(4).toInt
      (sp(0), (sp(1), sp(2), price, num, price * num))
    })
    //按照订单编号分组
    val group: RDD[(String, Iterable[(String, String, Float, Int, Float)])] = line.groupByKey()
    //把 值转成列表 按总价倒序排列  取前三
    val value: RDD[(String, List[(String, String, Float, Int, Float)])] = group.mapValues(x => {
      val list = x.toList
      val sort: List[(String, String, Float, Int, Float)] = list.sortBy(-_._5)
//      sort.take(3)
      sort
    })

    value.foreach(println)
//    value.saveAsTextFile("D:\\com.MapReduce\\Order\\spark_result")
    sc.stop()
  }
}
