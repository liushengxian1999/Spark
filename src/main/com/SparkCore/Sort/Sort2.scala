package Sort

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Sort2 {


  /**
   * 调用一个排序方法 实现排序
    */

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("Sort")

    val sc = new SparkContext(conf)

    var arr = Array("张三 13 1200","李四 20 1500", "王五 19 1200", "赵六 22 3000")

    // 转成RDD
    val ar = sc.parallelize(arr)

    //转成一个元组
    val data: RDD[(String, Int, Int)] = ar.map(x => {
      val line = x.split(" ")
      var name = line(0)
      var age = line(1).toInt
      var money = line(2).toInt
      (name, age, money)
    })
    // 可以只传入需要排序的字段（年龄，价格）  按照类中定义的排序方法排序
    val result: Array[(String, Int, Int)] = data.sortBy(x => MySort2(x._2,x._3)).collect()
    println(result.toBuffer)

  }
}

/**
 *样例类
 * 1、类前面加case
 * 2、自动实现序列化
 * 3、不需要new
 */
case class MySort2( age:Int, money:Int)  extends Ordered[MySort2]{
  // 排序规则:价格大的在前；如果价格相等，年龄小的在前
  override def compare(that: MySort2): Int = {

    if(this.money != that.money){
      that.money - this.money
    }else{
      this.age - that.age
    }
  }

}
