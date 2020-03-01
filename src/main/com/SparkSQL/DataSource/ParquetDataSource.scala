package DataSource

import org.apache.spark.sql.{DataFrame, SparkSession}

object ParquetDataSource {

  def main(args: Array[String]): Unit = {

    val session = SparkSession.builder().appName("ParquetDataSource")
      .master("local[*]")
      .getOrCreate()

    //指定以后读取json类型的数据
    val parquetLine: DataFrame = session.read.parquet("D:\\Spark_Test\\DataSource\\parquet")
    //val parquetLine: DataFrame = session.read.format("parquet").load("D:\Spark_Test\DataSource\parquet")

    parquetLine.printSchema()

    //show是Action
    parquetLine.show()

    session.stop()
  }
}
