package DataSource

import java.util.Properties

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object JdbcDataSource {

  def main(args: Array[String]): Unit = {

    val session = SparkSession.builder().appName("JdbcDataSource").master("local[3]").getOrCreate()

    import session.implicits._

    // 读取JDBC数据源 (只会读取schema信息   不会真正读取数据  )
    val jdbc: DataFrame = session.read.format("jdbc").options(
      Map("url" -> "jdbc:mysql://localhost:3306/bigdata",
        "driver" -> "com.mysql.jdbc.Driver",
        "dbtable" -> "user",
        "user" -> "root",
        "password" -> "123")
    ).load()

    //结构信息
//    jdbc.printSchema()



    //过滤
    //    val filter = jdbc.filter(x => {
    //      x.getAs[Int]("age") >= 14
    //    })


    // where 和filter 调用的用一种方法
    //    val filter = jdbc.where($"age" >= 14)
    //    val filter = jdbc.filter($"age" >= 14)

    val result: DataFrame = jdbc.select($"id", $"name" ,$"age" * 10 as "age")


    //-----------------------写出数据-----------------------

    //将数据写入数据库
    val props = new Properties()
    props.put("user","root")
    props.put("password","123")
    // ignore 表存在不操作；如果不存在则创建表
    result.write.mode("ignore").jdbc("jdbc:mysql://localhost:3306/bigdata", "use2", props)

    //DataFrame保存成text时出错(只能保存一列 只能保存string类型)
    result.write.text("D:\\Spark_Test\\DataSource\\text")

    //写入json
    result.write.json("D:\\Spark_Test\\DataSource\\json")
    //csv
    result.write.csv("D:\\Spark_Test\\DataSource\\csv")
    //parquet
    result.write.parquet("D:\\Spark_Test\\DataSource\\parquet")


    session.stop()

  }
}
