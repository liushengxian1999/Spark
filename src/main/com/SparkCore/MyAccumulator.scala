package com.SparkCore

import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 自定义累加器
 */

object MyAccumulator {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Accumulator").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val data: RDD[String] = sc.makeRDD(List("lsx", "ltz", "apple", "banana"))

     // 自定义累加器使用
     // 1、创建累加器
    val wa = new WordAccumulator
    //  2、向sc注册累加器
    sc.register(wa)
    data.foreach(x=>wa.add(x))
    print(s"sum = ${wa.value}") //sum = [apple, lsx, ltz]

  }
}


/**
 * 自定义累加器：
 * 将含有'l'的单词放到一个list中
 * */
class WordAccumulator extends AccumulatorV2[String,util.ArrayList[String]]{

  private val list = new util.ArrayList[String]()

  // 判断当前的累加否为初始化状态
  override def isZero: Boolean = {
    list.isEmpty
  }

  // 复制累加器对象
  override def copy(): AccumulatorV2[String, util.ArrayList[String]] = {
   new WordAccumulator
  }

  // 重置累加器对象
  override def reset(): Unit = {
    list.clear()
  }

  // 向累加器中增加数据
  override def add(v: String): Unit = {
    if(v.contains("l")){
      list.add(v)
    }
  }

  // 合并累加器
  override def merge(other: AccumulatorV2[String, util.ArrayList[String]]): Unit = {
    list.addAll(other.value)
  }

  // 获取累加器的结果
  override def value: util.ArrayList[String] = {
    list
  }
}
