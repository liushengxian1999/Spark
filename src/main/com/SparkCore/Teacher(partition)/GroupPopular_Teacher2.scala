package Teacher

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object GroupPopular_Teacher2 {


  def main(args: Array[String]): Unit = {

    //数据样例：http://bigdata.edu360.cn/laozhang

    /**
     * 思路：先以（学科，老师）为key 进行聚合操作
     *      然后 把每个学科的数据过滤出来  进行排序取值
     *      优点：可以进行大规模数据操作
     *      缺点：触发多次action
     */
    val conf = new SparkConf().setAppName("Teacher").setMaster("local[3]")
    val sc = new SparkContext(conf)
    val text = sc.textFile("D:\\Spark_Test\\Teacher\\data")

    // 定义函数，index=2时返回学科   =3时返回老师名字
    var f1 = (x:String, index:Int) =>{
      if(index == 2){
        x.split("/")(2).split("\\.")(0)
      }else{
        x.split("/")(3)
      }
    }

    //(学科，老师），1
    val map = text.map(x=> ((f1(x,2), f1(x,3)), 1))

    // 聚合
    val reduce: RDD[((String, String), Int)] = map.reduceByKey(_ + _)

    // 加载到内存(标记为cache的RDD以后被反复使用 才使用cache)
    val cache = reduce.cache()

    val sName = Array("bigdata", "php", "javaee")

    // 把 每个学科的所有数据过滤出来
    for (elem <- sName) {

      // 每个学科所有的值
        val value: RDD[((String, String), Int)] = cache.filter(_._1._1 == elem)

      // 排序取结果
      val result: Array[((String, String), Int)] = value.sortBy(_._2,false).take(1)
      result.foreach(println)

    }
  }
}
