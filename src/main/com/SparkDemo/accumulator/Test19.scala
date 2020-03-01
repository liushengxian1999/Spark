

package com.SparkDemo.accumulator

import org.apache.spark.{SparkConf, SparkContext}

object Test19 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("acculator")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(Seq("hello", "tom", "jerry", "marry",
      "zhangsan", "lisi", "jams", "tom", "jerry", "marry", "zhangsan",
      "lisi", "jams", "tom", "jerry", "marry", "zhangsan", "lisi", "jams",
      "tom", "jerry", "marry", "zhangsan", "lisi", "jams", "tom", "jerry", "marry",
      "zhangsan", "lisi", "jams", "tom", "jerry", "marry", "zhangsan", "lisi", "jams",
      "tom", "jerry", "marry", "zhangsan", "lisi", "jams", "tom", "jerry", "marry", "zhangsan",
      "lisi", "jams", "tom", "jerry", "marry", "zhangsan", "lisi", "jams"))
      println(rdd1.partitions.length)
    val acc = sc.accumulator(0) //acc就是累加器
    val rdd2 = rdd1.map { x =>
      {
        acc.add(1);
        (x, 1)
      }
    }
    val count = rdd2.count()
    println(count + "    " + acc.value)

  }
}