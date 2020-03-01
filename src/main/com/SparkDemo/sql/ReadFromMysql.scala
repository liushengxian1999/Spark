package com.SparkDemo.sql

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}

object ReadFromMysql {

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().master("local[3]").appName("create").getOrCreate()
    import session.implicits._

//    val frame: DataFrame = session.read.format("jdbc").options(Map("url" -> "jdbc:mysql://localhost:3306/test?characterEncoding=utf8",
    //      "dbtable" -> "df",
    //      "com.SparkSQL.SQL2.user" -> "root",
    //      "password" -> "123"
    //    )).load()

    val pro = new Properties()
    pro.put("user","root")
    pro.put("password","123")
    val frame = session.read.jdbc("jdbc:mysql://localhost:3306/test?characterEncoding=utf8", "df", pro)
    frame.show()
  }
}
