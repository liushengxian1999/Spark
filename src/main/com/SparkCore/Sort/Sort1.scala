package Sort

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Sort1 {


  /**
   * 放到对象里 调用order方法实现自定义排序
    */

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("Sort")
    val sc = new SparkContext(conf)

    var arr = Array("张三 13 1200","李四 20 1500", "王五 19 1200", "赵六 22 3000")

    // 转成RDD
    val ar = sc.parallelize(arr)

    //排序(实例化成一个对象)
    val s: RDD[MySort] = ar.map(x => {
      val line = x.split(" ")
      var name = line(0)
      var age = line(1).toInt
      var money = line(2).toInt
      new MySort(name, age, money)
    })

    // 按照 类中定义的排序方法排序
    val result: Array[MySort] = s.sortBy(x => x).collect()
    println(result.toBuffer)


  }
}

/**
 *注意：
 * 1、继承Ordered
 * 2、实现序列化
 */

class MySort(val name:String,val age:Int,val money:Int)  extends Ordered[MySort] with Serializable {
  // 排序规则:价格大的在前；如果价格相等，年龄小的在前
  override def compare(that: MySort): Int = {

    if(this.money != that.money){
      that.money - this.money
    }else{
      this.age - that.age
    }
  }

  override def toString: String = s"name:$name , age:$age , money:$money"
}
