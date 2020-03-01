package com.SparkSQL.SQL2

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object SQL_wordcount {

  def main(args: Array[String]): Unit = {

    val session = SparkSession.builder().appName("wordcount").master("local[3]").getOrCreate()

    // dataset默认只有一行
    val lines: Dataset[String] = session.read.textFile("D:\\Spark_Test\\wordcount")

    //隐式转换
    import session.implicits._

    val result: Dataset[String] = lines.flatMap(_.split(" "))
    //注册临时试图
    result.createTempView("v")
    // 用SQL
    val end: DataFrame = session.sql("select value,count(1) as c from v group by value order by c desc ")
    end.show()
    session.stop()

  }
}
