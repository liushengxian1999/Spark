package Sort

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Sort4 {

  /**
   *  运用元组自定义的排序规则 ：先比第一个 （默认升序） 相等再比第二个
   */

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("Sort")
    val sc = new SparkContext(conf)

    var arr = Array("张三 13 1200","李四 20 1500", "王五 19 1200", "赵六 22 3000")

    // 转成RDD
    val ar = sc.parallelize(arr)

    //(转成一个元组）
    val s = ar.map(x => {
      val line = x.split(" ")
      var name = line(0)
      var age = line(1).toInt
      var money = line(2).toInt
      (name, age, money)
    })

    // 按照元组 自定义排序规则： 先比第一个 （默认升序） 相等再比第二个
    val result = s.sortBy(x=>(-x._3,x._2)).collect()
    println(result.toBuffer)

  }
}





