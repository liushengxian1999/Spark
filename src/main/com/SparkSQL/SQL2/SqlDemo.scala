package com.SparkSQL.SQL2

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object SqlDemo {


  def main(args: Array[String]): Unit = {

    //建立sparksession
    val session = SparkSession.builder()
                              .appName("sql")
                              .master("local[3]")
                              .getOrCreate()

    val lines = session.sparkContext.textFile("D:\\Spark_Test\\data")

    // 数据进行整理
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

    val df: DataFrame = session.createDataFrame(row,schema)

    //隐式转换
    import session.implicits._
    val df2 = df.where($"age" >20).sort($"age" desc)
    df2.show()
    session.stop()
  }
}
