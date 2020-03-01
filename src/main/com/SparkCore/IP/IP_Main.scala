package IP

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object IP_Main {


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("IP").setMaster("local[4]")
    val sc = new SparkContext(conf)

    // 获得规则
    val rules: Array[(Long, Long, String)] = MyUtil.SetRules("D:\\Spark_Test\\IP\\data\\ip.txt")

    //广播的方法的引用
    val broadRef: Broadcast[Array[(Long, Long, String)]] = sc.broadcast(rules)


    var f1 = (x:String)=>{

      //切分IP
      val data = x.split("\\|")(1)
      //转成十进制
      val ip: Long = MyUtil.IP2Ten(data)

      //二分法查找取得该IP的有关信息下标
          //通过引用获得IP规则
      val rule: Array[(Long, Long, String)] = broadRef.value
          //下标
      val index: Int = MyUtil.binSearch(rule,ip)

      var info = "未知"
      // 判断信息是否有效
      if(index != -1){
        info = rule(index)._3
      }
      (info,1)
    }


    //读取log日志，获得IP，转成十进制，查找该IP信息
    val lines: RDD[String] = sc.textFile("D:\\Spark_Test\\IP\\data\\access.log")
    val data: RDD[(String, Int)] = lines.map(x => f1(x))
    // 聚合
    val result =  data.reduceByKey(_+_)

    result.foreachPartition(x => MyUtil.down2mysql(x))
    sc.stop()


  }

}
