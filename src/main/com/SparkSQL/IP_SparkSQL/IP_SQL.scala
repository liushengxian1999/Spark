package IP_SparkSQL

import IP.MyUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object IP_SQL {

  def main(args: Array[String]): Unit = {

    /**
     * 建立两个表：
     * 规则表(起始，终止，省份)
     * 日志表（IP）
     *
     * 缺点：
     * Join太多
     */

    val session = SparkSession.builder().appName("IPSQL").master("local[4]").getOrCreate()

    import session.implicits._

    //读取IP规则
    val rule: Dataset[String] = session.read.textFile("D:\\Spark_Test\\IP\\data\\ip.txt")
    //获得规则表（转成DataFrame ，直接在toDF里面添加列名 ）
    val ruleDF = rule.map(x => {

      val line = x.split("\\|")
      val start: Long = line(2).toLong
      val end: Long = line(3).toLong
      val sheng = line(6)

      (start, end, sheng)
    }).toDF("start", "end", "province")

    // 读取日志
    val log = session.read.textFile("D:\\Spark_Test\\IP\\data\\access.log")
    //获得IP（添加列名）
    val logDF: DataFrame = log.map(x => {
      //切分IP
      val data = x.split("\\|")(1)
      //转成十进制
      val ip: Long = MyUtil.IP2Ten(data)
      ip
    }).toDF("IP")


    //注册成临时视图
    ruleDF.createTempView("rule")
    logDF.createTempView("log")

    val end: DataFrame = session.sql("select province,count(*) as count from rule,log where IP >= start and IP <= end group by province")
    end.show()



  }
}
