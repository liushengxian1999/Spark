package com.SparkSQL

import org.apache.spark.sql.SparkSession

object Spark_Hive {

  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession.builder()
                          .appName("hive")
                          .master("local[*]")
                          .enableHiveSupport()
                          .getOrCreate()
    session.sql("show databases").show()

  }
}
