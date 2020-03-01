package DataSource

import org.apache.spark.sql.{DataFrame, SparkSession}

object CsvDataSource {

  def main(args: Array[String]): Unit = {

    val session = SparkSession.builder().appName("CsvDataSource")
      .master("local[*]")
      .getOrCreate()

    //指定以后读取json类型的数据
    val csv: DataFrame = session.read.csv("D:\\Spark_Test\\DataSource\\csv")
    // 默认全部是string类型
    csv.printSchema()

    // 加入schema
    val pdf: DataFrame = csv.toDF("id", "name", "age")

    pdf.show()

    session.stop()

  }
}
