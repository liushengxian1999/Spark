package com.SparkDemo.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object Save2Mysql {

  /**
   * 读数据 写到数据库中
   */
  def main(args: Array[String]): Unit = {

    val session = SparkSession.builder().master("local[3]").appName("create").getOrCreate()
    import session.implicits._
    val df: DataFrame = session.read.format("text").load("text.txt")
    val result: DataFrame = df.rdd.map(x => {
      val words: Array[String] = x.toString().split(" ")
      (words(0) + " " + words(1), words(2))
    }).toDF("date", "session")

    result.show()

    result.write.mode("append").format("jdbc").options(Map("url"->"jdbc:mysql://localhost:3306/test?characterEncoding=utf8",
      "dbtable"->"df",
      "user"->"root",
      "password"->"123"
    )).save()

  }

}
