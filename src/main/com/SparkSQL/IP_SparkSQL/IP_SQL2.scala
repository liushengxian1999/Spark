package IP_SparkSQL

import IP.MyUtil
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object IP_SQL2 {

  def main(args: Array[String]): Unit = {

    /**
     *
     * 改进：
     * 将数据广播出去  一个Executor有一个引用 减少shuffle
     * 创建自定义函数
     */

    val session = SparkSession.builder().appName("IPSQL2").master("local[4]").getOrCreate()

    import session.implicits._

    val rule: Dataset[String] = session.read.textFile("D:\\Spark_Test\\IP\\data\\ip.txt")

    //获得规则
    val rules: Dataset[(Long, Long, String)] = rule.map(x => {

      val line = x.split("\\|")
      val start: Long = line(2).toLong
      val end: Long = line(3).toLong
      val sheng = line(6)
      (start, end, sheng)
    })
    //将规则广播出去（只有SparkContext才能广播）
    val ruleArr: Array[(Long, Long, String)] = rules.collect()
    val ruleRef = session.sparkContext.broadcast(ruleArr)

    // 读取日志
    val log = session.read.textFile("D:\\Spark_Test\\IP\\data\\access.log")

    //添加列名 ：IP
    val logDF: DataFrame = log.map(x => {

      //切分IP
      val data = x.split("\\|")(1)
      //转成十进制
      val ip: Long = MyUtil.IP2Ten(data)
      ip
    }).toDF("IP")

    //注册临时表
    logDF.createTempView("log")

    //创建自定义函数 参数：IP    返回：省份
    session.udf.register("IP2Province", (x:Long) =>{

      val rules: Array[(Long, Long, String)] = ruleRef.value
      val index: Int = MyUtil.binSearch(rules, x)
      var province = "未知"
      if(index != -1){
        province = rules(index)._3
      }
      province
    })

    // 自定义函数   参数是IP  返回 省份
    val end: DataFrame = session.sql("select IP2Province(IP) as province,count(*) as count from log GROUP BY province ORDER BY province desc")
    end.show()

    session.stop()


  }
}
