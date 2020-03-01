package com.SparkSQL.SQL1

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object SqlDemo1 {
  /**
   * 创建 sparkcontext  sqlcontext
   * 把数据和case class相关联 然后转成DataFrame
   *
   * DataFrame注册成临时表  然后操作
   */

  def main(args: Array[String]): Unit = {

      val conf = new SparkConf().setAppName("demo").setMaster("local[2]")

    val sc = new SparkContext(conf)
    // 包装
    val sql = new SQLContext(sc)

    val lines = sc.textFile("D:\\Spark_Test\\data")

    // 和case class相关联
    val map: RDD[person] = lines.map(x => {

      val line = x.split(",")
      var id = line(0).toInt
      var name = line(1)
      var age = line(2).toInt
      var fv = line(3).toInt
      new person(id, name, age, fv)
    })

    // 把RDD转换成DataFrame （导入隐式转换）
    import sql.implicits._
    val dataframe: DataFrame = map.toDF

    // 把DataFrame注册成临时表
    dataframe.registerTempTable("t_boy")

    val result: DataFrame = sql.sql("select * from t_boy")

    //展示
    result.show()
    sc.stop()

  }
}

case class person(id:Int, name:String, age:Int, fv:Int){

}