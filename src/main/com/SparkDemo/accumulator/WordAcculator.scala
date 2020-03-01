package com.SparkDemo.accumulator

import org.apache.spark.AccumulatorParam

import scala.collection.mutable.Map

//自定义累加器，而且value是Map[String,Int]，实际上并没有把zero的返回值给r1
class WordAcculator extends AccumulatorParam[Map[String, Int]] {
  //r1总结果  r2当前结果  Map("hello"->1)
  def addInPlace(r1: Map[String, Int], r2: Map[String, Int]): Map[String, Int] = {
    if (r1 == null) {     
      r2
    } else {     
      val key = r2.keys.iterator.next();
      val count = r1.getOrElse(key, 0)
      r1.+=((key, count + 1))
      r1
    }    
  }
  //这个方法在累加数据之前被调用一次
  def zero(initialValue: Map[String, Int]): Map[String, Int] = {
    Map("hello" -> 0, "tom" -> 0, "jerry" -> 0, "marry" -> 0, "zhangsan" -> 0, "lisi" -> 0, "jams" -> 0)
  }

}