import java.text.DecimalFormat

import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, RelationalGroupedDataset, Row, SparkSession}

import scala.collection.mutable

object Average {


  def main(args: Array[String]): Unit = {

    /**
     * 统计每门课程参考学生的平均分，
     * 并且按课程存入不同的结果文件，
     * 要求一门课程一个结果文件，
     * 并且按平均分从高到低排序，
     * 分数保留一位小数。
     * eg：
     * computer,黄晓明,85,86,41,75,93,42,85
     * computer,徐峥,54,52,86,91,42
     *
     * 1、读文件 建立分区器
     * 2、求平均分（保留一位小数）
     */

    val conf = new SparkConf().setAppName("average").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("D:\\com.MapReduce\\average\\data")

    // 科目总数（有几个结果文件）
    val subject: Array[String] = lines.map(_.split(",")(0)).distinct().collect()

    val partition = new MyPartition(subject)

    // 工具函数 （计算平均数，整合数据）
    var f1 = (x:String) =>{

      //计算平均数（保留一位小数）
      val sp = x.split(",")
      var sum:Float = 0
      for(i <- 2 until sp.length){
        sum += sp(i).toFloat
      }
      //平均数 保留一位小数
      val avg = sum / (sp.length-2)
      val format = new DecimalFormat("#.0")
      val avg_1 = format.format(avg).toFloat

      //整合数据(科目,(姓名,平均分))
      (sp(0), (sp(1), avg_1 ))
    }

    val map: RDD[(String, (String, Float))] = lines.map(f1(_)).sortBy(_._2._2, false)
    val result: RDD[(String, (String, Float))] = map.partitionBy(partition)
    result.saveAsTextFile("D:\\com.MapReduce\\average\\spark_end")

    sc.stop()
  }
}

class MyPartition (arr:Array[String] ) extends Partitioner{

  private val hash = new mutable.HashMap[String,Int]()
  var i = 0;
  for (elem <- arr) {
    hash(elem) = i
    i+=1
  }

  override def numPartitions: Int = arr.length

  override def getPartition(key: Any): Int = {

    val str = key.asInstanceOf[String]
    if (hash.contains(str)) hash(str) else arr.length

  }
}