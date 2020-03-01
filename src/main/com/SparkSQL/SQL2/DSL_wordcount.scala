package com.SparkSQL.SQL2

import org.apache.spark.sql.{Dataset, Row, SparkSession}

object DSL_wordcount {

  def main(args: Array[String]): Unit = {

    val session = SparkSession.builder().appName("wordcount").master("local[3]").getOrCreate()

    // dataset默认只有一行
    val lines: Dataset[String] = session.read.textFile("D:\\Spark_Test\\wordcount")

    //隐式转换
    import session.implicits._
    val result: Dataset[String] = lines.flatMap(_.split(" "))

    // 使用DataSet(DSL)
    val end: Dataset[Row] = result.groupBy($"value" as "word").count().sort($"count" desc)
    end.show()
  }

}
