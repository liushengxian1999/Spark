package com.SparkSQL.SQL1

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object SqlDemo2 {
  /**
   * 创建 sparkcontext  sqlcontext
   * 把数据和Row相关联  创建StructType(schema)
   * 调用 createDataFrame 创建DataFrame
   * DataFrame注册成临时表  然后使用SQL操作
   */

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("demo").setMaster("local[2]")

    val sc = new SparkContext(conf)
    // 包装
    val sqlcontext = new SQLContext(sc)

    val lines = sc.textFile("D:\\Spark_Test\\data")

    // 数据进行整理 和Row相关联
    val row: RDD[Row] = lines.map(x => {
      val line = x.split(",")
      var id = line(0).toInt
      var name = line(1)
      var age = line(2).toInt
      var fv = line(3).toInt
      Row(id, name, age, fv)
    })

    //建立schema(结果类型   表头字段)
    val schema: StructType = StructType(List(
      StructField("id", IntegerType, true),
      StructField("name", StringType, true),
      StructField("age", IntegerType, true),
      StructField("fv", IntegerType, true)
    ))

    // 创建dataframe
    val dataframe: DataFrame = sqlcontext.createDataFrame(row,schema)

    // 把DataFrame注册成临时表
    dataframe.registerTempTable("t_boy")

    val result: DataFrame = sqlcontext.sql("select * from t_boy")

    //展示
    result.show()
    sc.stop()

  }
}
