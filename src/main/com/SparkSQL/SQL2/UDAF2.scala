package com.SparkSQL.SQL2

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Aggregator

object UDAF2 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("UDAF").setMaster("local[*]")
    val session: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    import session.implicits._

    // 创建聚合函数(不能用register：因为输入类型是UserBean)
    val avgFunction = new MyAvgFunction2
    // 将聚合函数转换为查询列
    val col: TypedColumn[UserBean, Double] = avgFunction.toColumn.name("avgCol")

    //读取数据
    val line: DataFrame = session.read.json("in")
    // 转换为DS
    val ds: Dataset[UserBean] = line.as[UserBean]
    val result: DataFrame = ds.select(col)
    result.show()

  }
}


case class UserBean(name:String,age:Long)
case class AvgBuffer(var sum:Long,var count:Long)
/**
 * 自定义聚合函数(强类型)
 */
class MyAvgFunction2 extends Aggregator[UserBean,AvgBuffer,Double]{

  // 初始化
  override def zero: AvgBuffer = AvgBuffer(0L,0L)

  // 传来新数据，计算
  override def reduce(b: AvgBuffer, a: UserBean): AvgBuffer = {
    b.sum = b.sum + a.age
    b.count = b.count + 1
    b
  }

  // 合并
  override def merge(b1: AvgBuffer, b2: AvgBuffer): AvgBuffer = {
    b1.sum = b1.sum + b2.sum
    b1.count = b1.count + b2.count
    b1
  }

  // 返回
  override def finish(reduction: AvgBuffer): Double = {
    reduction.sum.toDouble / reduction.count
  }

  override def bufferEncoder: Encoder[AvgBuffer] = Encoders.product

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}
