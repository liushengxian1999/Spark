package Teacher

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object GroupPopular_Teacher {
  /**
   * (分组Top N)
   * 求每个学科中最受欢迎老师的top3
   * 思路：
   * 先以（学科，老师）为key 进行聚合操作
   * 然后以学科为key进行分组
   * 再把数据转成list进行排序  （转成List就是 转到内存计算）
   * 优点：触发一次action
   * 缺点：不能大规模数据计算
   */

  def main(args: Array[String]): Unit = {
    //数据样例：http://bigdata.edu360.cn/laozhang

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

    // 解析成 （(学科，老师)，1）
   val SubjectAndTeacher: RDD[((String, String), Int)] = text.map(x=>( (f1(x,2), f1(x,3)), 1))

    // 聚合
    val reduce = SubjectAndTeacher.reduceByKey(_+_)
    /*
    ((php,laoliu),1)
    ((javaee,xiaoxu),6)
    ((bigdata,laozhang),2)
    ((bigdata,laozhao),15)
    ((javaee,laoyang),9)
    ((php,laoli),3)
    ((bigdata,laoduan),6)
     */

    //按学科分组
    val group: RDD[(String, Iterable[((String, String), Int)])] = reduce.groupBy(x => x._1._1)

    /*
        (javaee,CompactBuffer(((javaee,xiaoxu),6), ((javaee,laoyang),9)))
        (php,CompactBuffer(((php,laoliu),1), ((php,laoli),3)))
        (bigdata,CompactBuffer(((bigdata,laozhang),2), ((bigdata,laozhao),15), ((bigdata,laoduan),6)))
     */

    val result = group.mapValues(x => x.toList.sortBy(_._2).reverse.take(1))
    result.collect().foreach(println)
    /*
        (javaee,List(((javaee,laoyang),9)))
        (php,List(((php,laoli),3)))
        (bigdata,List(((bigdata,laozhao),15)))
     */





  }

}
