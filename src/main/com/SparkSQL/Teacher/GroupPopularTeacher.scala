package com.SparkSQL.Teacher

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object GroupPopularTeacher {

  // 分组TopN
  def main(args: Array[String]): Unit = {

    val session = SparkSession.builder().appName("teacher").master("local[2]").getOrCreate()

    val lines: Dataset[String] = session.read.textFile("D:\\Spark_Test\\Teacher\\data")

    // 整合函数
    var f1 = (x:String, index:Int) =>{
      if(index == 2){
        x.split("/")(2).split("\\.")(0)
      }else{
        x.split("/")(3)
      }
    }

    import session.implicits._

    val data: Dataset[(String, String)] = lines.map(x => (f1(x, 2), f1(x, 3)))
    val df = data.toDF("Subject", "Teacher")

    df.createTempView("first")

    val second: DataFrame = session.sql("SELECT *,count(*) as counts FROM first GROUP BY Subject,Teacher")

    second.createTempView("second")

    val third: DataFrame = session.sql("SELECT Subject,Teacher,counts FROM (SELECT *,row_number() over(partition by Subject order by counts desc) as g_num FROM second) tmp WHERE g_num =1")
//    third.write.text("D:\\Spark_Test\\Teacher\\json")
    third.show()
    session.stop()


  }
}
