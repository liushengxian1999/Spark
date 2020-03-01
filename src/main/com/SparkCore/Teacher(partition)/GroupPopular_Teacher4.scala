package Teacher

import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.mutable

object GroupPopular_Teacher4 {
  def main(args: Array[String]): Unit = {
    //数据样例：http://bigdata.edu360.cn/laozhang

    /**
     * 思路：
     * 先聚合  然后把每个学科放到一个分区  然后取出每个分区的数据进行排序

     * 方法：
     * 写一个自定义分区器
     * 主构造方法 传入 学科数组
     * 在聚合的时候进行分区  减少shuffle
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



    // 把所有学科存放到一个数组
    val subs: Array[String] = text.map(x => ((f1(x,2),f1(x,3)), 1)).map(_._1._1).distinct().collect()

    // 自定义一个分区器
    val MyPartitioner = new MyPartitioner2(subs)

    //转成(学科，老师），1   再聚合(聚合是可以指定分区器)（聚合时可以调用分区器）
    val partition: RDD[((String, String), Int)] = text.map(x => ((f1(x,2),f1(x,3)), 1)).reduceByKey(MyPartitioner,_+_)


    // 取出每个分区进行排序
    val data: RDD[((String, String), Int)] = partition.mapPartitions(x => x.toList.sortBy(_._2).reverse.take(1).iterator)

//    data.saveAsTextFile("D:\\Spark_Test\\Teacher\\end")
    data.collect().foreach(println)
  }

}

class MyPartitioner2(sbs:Array[String] ) extends Partitioner{

  // hash存放 学科 编号
  private val hash = new mutable.HashMap[String,Int]()

  var i = 0;
  for (elem <- sbs) {

    hash(elem) = i
    i += 1
  }

  //有几个分区
  override def numPartitions: Int = sbs.length
  //按key进行分区
  override def getPartition(key: Any): Int = {

    // 每次传入的key是一个元祖 （String,String）
    val subject = key.asInstanceOf[(String,String)]._1

    hash(subject)

  }
}
