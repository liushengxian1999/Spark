import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object Join {

  def main(args: Array[String]): Unit = {

    val session = SparkSession.builder().appName("Join").master("local[3]").getOrCreate()

    //读取数据
    val user = session.sparkContext.textFile("D:\\Spark_Test\\Join\\data\\users.txt")
    val order: RDD[String] = session.sparkContext.textFile("D:\\Spark_Test\\Join\\data\\order.txt")

    var f1 = (x:String,num:Int) =>{
      val arr = x.split(",")
      if (num == 2){
        Row(arr(0),arr(1))
      }else{
        Row(arr(0),arr(1),arr(2).toInt)
      }
    }

    // 数据和Row关联
    val u_Row = user.map(f1(_,3))
    val o_Row = order.map(f1(_,2))

    //建立schem
    val uType = StructType(List(
      StructField("uid", StringType, true),
      StructField("name", StringType, true),
      StructField("price", IntegerType, true)
    ))

    val oType = StructType(List(
      StructField("oid", StringType, true),
      StructField("uid", StringType, true)
    ))

    //建立dataframe
    val uFrame: DataFrame = session.createDataFrame(u_Row,uType)
    val oFrame: DataFrame = session.createDataFrame(o_Row, oType)

    //创建临时视图
    uFrame.createTempView("user")
    oFrame.createTempView("order")

    val result: DataFrame = session.sql("select o.oid,u.uid,name,price from com.SparkSQL.SQL2.user as u,order as o " +
                                                              "where u.uid = o.uid " +
                                                                "order by o.oid")
    result.show()
    session.stop()
  }
}
