package Teacher

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object The_MostPopular_Teacher {
  /**在所有的老师中求出最受欢迎的老师Top3
    （全局排序）
   */
  def main(args: Array[String]): Unit = {

    //数据样例：http://bigdata.edu360.cn/laozhang
    val conf = new SparkConf().setAppName("Teacher").setMaster("local[2]")
    val sc = new SparkContext(conf)
    // 读取数据源
    val text = sc.textFile("D:\\Spark_Test\\Teacher\\data")

    // 映射成 (名字，1)
    val map: RDD[(String, Int)] = text.map(x => (x.split("/").last, 1))

    // 聚合 (名字，受欢迎程度)
    val reduce = map.reduceByKey(_+_)

    //降序排序
    val sort: RDD[(String, Int)] = reduce.sortBy(x => x._2,false)

    //取前三
    val data = sort.take(3).map(x => x._1+"\t"+x._2)

    data.foreach(println)

  }
}
