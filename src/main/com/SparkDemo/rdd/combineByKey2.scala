package com.SparkDemo.rdd

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import scala.collection.Set

object Test2 {
  def main(args: Array[String]): Unit = {
    //1.我想求每个会话时间最大值和最小值
    //2.求每天的会话数
    //reduceByKey用计算数字运算聚合，
    //2012-12-12 05:34:56   session1     
    //2012-12-12 05:35:56  session1
    //2012-12-12 05:38:56  session1
    
    //2012-12-12 05:40:56  session1
    //2012-12-12 05:50:56  session1
    
    
    //2012-12-12 05:34:56   session2     
    //2012-12-12 05:35:56  session2
    //2012-12-12 05:38:56  session2
    
    //2012-12-12 05:40:56  session2
    //2012-12-12 05:50:56  session2
    val conf=new SparkConf()
    conf.setAppName("session")
    conf.setMaster("local")
    val sc=new SparkContext(conf)
    //创建 RDD的第二种方式 
    val rddText=sc.textFile("file:///d:/111/text.txt")
    val rddSessionIDAndDate=rddText.map { x =>{
      val values=x.split(" ") 
      (values(0),values(2))
    }}
    //combineByKey 当想把值转换成集合
    //(session1 ,List[](2012-12-12 05:34:56,2012-12-12 05:35:56,2012-12-12 05:38:56,2012-12-12 05:40:56,2012-12-12 05:50:56))
                                                         //同分区同key第一个元素 实现类型转换                    同key第二元素之后所有元素集合                             分区之间
    val rddReduceByKey=rddSessionIDAndDate.combineByKey((x:String)=>Set[String](x), (a:Set[String],b:String)=>a.+(b), (c:Set[String],d:Set[String])=>c++d)
    val rddReduceByKeyToList=rddReduceByKey.map(f=>(f._1,f._2.size))
    rddReduceByKeyToList.foreach(println)
    
  }
}