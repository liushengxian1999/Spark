package com.SparkSQL.SQL2

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object UDAF {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("UDAF").setMaster("local[*]")
    val session: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val line: DataFrame = session.read.json("in")

    val myAvg: MyAvgFunction = new MyAvgFunction
    session.udf.register("myAvg",myAvg)

    line.createTempView("user")

    val result: DataFrame = session.sql("SELECT myAvg(age) from user")
    result.show()

  }
}

/**
 * 自定义聚合函数
 */
class MyAvgFunction extends UserDefinedAggregateFunction{

  // 函数输入的数据结构
  override def inputSchema: StructType = new StructType().add("age",LongType)

  // 计算的数据结构
  override def bufferSchema: StructType = new StructType().add("sum",LongType).add("count",LongType)

  // 函数返回的数据类型
  override def dataType: DataType = DoubleType

  // 是否稳定
  override def deterministic: Boolean = true

  // 计算之前缓冲区初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = 0L
  }

  // 当数据传过来时 更新数据
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getLong(0) + input.getLong(0)
    buffer(1) = buffer.getLong(1) + 1
  }

  // 将多个节点的缓冲区合并
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  // 返回结果
  override def evaluate(buffer: Row): Any = {
    buffer.getLong(0).toDouble / buffer.getLong(1)
  }
}
