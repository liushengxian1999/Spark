package DataSource

import org.apache.spark.sql.{DataFrame, SparkSession}

object JsonDataSource {

  def main(args: Array[String]): Unit = {


    val session = SparkSession.builder().appName("JdbcDataSource")
      .master("local[*]")
      .getOrCreate()

    import session.implicits._

    //指定以后读取json类型的数据(有表头)
    val jsons: DataFrame = session.read.json("D:\\Spark_Test\\DataSource\\json")

    val filtered: DataFrame = jsons.where($"age" <= 500)


    filtered.printSchema()

    filtered.show()

    session.stop()

  }
}
