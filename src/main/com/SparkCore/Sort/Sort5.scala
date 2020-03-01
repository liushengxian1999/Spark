package Sort

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Sort5 {


  /**
   * 使用隐式转换
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

    // 优先按照隐式转换
    // Ordering[(Int,Int)] 最终比较的数据格式
    //on[(String,Int,Int)]未比较前的数据格式
    //(t => (-t._3, t._2)) 怎样将规则转换成想要比较的格式
    implicit val rules = Ordering[(Int,Int)].on[(String,Int,Int)](t => (-t._3, t._2))
    val result = s.sortBy(x=>x).collect()
    println(result.toBuffer)

  }
}





