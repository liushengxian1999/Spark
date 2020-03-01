package com.SparkSQL.SQL2

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object UDF {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("UDF").setMaster("local[*]")
    val session: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val line: DataFrame = session.read.json("in")
    // 自定函数：查询结果前面加 Name:
    session.udf.register("addName", (name:String) =>{
      "Name:"+name
    })

    line.createTempView("user")

    val result: DataFrame = session.sql("SELECT addName(name) from user")
    result.show()

  }

}
