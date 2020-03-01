package JDBC_RDD

import java.sql.DriverManager

import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}

object JDBC {

  def main(args: Array[String]): Unit = {


    var getcon = () =>{
      DriverManager.getConnection("jdbc:mysql://localhost:3306/com.SparkSQL.SQL2.user","root","123")
    }


    val conf = new SparkConf().setAppName("jdbc").setMaster("local[*]")
    val sc = new SparkContext(conf)

    //JdbcRdd
    val JDBCRD: RDD[(Int, String, Int)] = new JdbcRDD(
      sc, //sparkcontext
      getcon,//获得连接
      "select * from php where id>=? and id <=?", // sql语句
      1,//起始边界
      5,//结束边界
      2, //分区数量
      rs => { // 结果集转换
        val id = rs.getInt(1)
        val name = rs.getString(2)
        val age = rs.getInt(3)
        (id, name, age)
      }
    )

    val result = JDBCRD.collect()
    println(result.toBuffer)
    //ArrayBuffer((1,c,12), (2,d,11), (3,e,31), (4,f,12), (5,TT,12))
    sc.stop()

    //select * from php where id>=? and id <?"
    //ArrayBuffer((1,c,12), (3,e,31), (4,f,12))

  }



}
