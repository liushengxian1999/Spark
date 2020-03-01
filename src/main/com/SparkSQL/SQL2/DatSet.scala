package com.SparkSQL.SQL2

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object DatSet {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("dataset").setMaster("local[*]")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    val data: RDD[(String, Int)] = spark.sparkContext.makeRDD(List(("bob", 12), ("alice", 16), ("tina", 19)))

    /**
    * RDD -> DF
     */
    val df: DataFrame = data.toDF("name", "age")
//    df.show()


    /**
     * RDD -> DS
     */
    val ds: Dataset[user] = data.map(x => {
      user(x._1, x._2)
    }).toDS()
//    ds.show()


    /**
     * DF -> RDD
     */
    val rdd: RDD[Row] = df.rdd
//    rdd.foreach(x =>{
//      println(x.getString(0))
//    })

    /**
     * DF -> DS
     */
    val value: Dataset[user] = df.as[user]
//    value.show()

    /**
     * DS -> RDD
     */
    val rdd1: RDD[user] = ds.rdd

    /**
     * DS -> DF
     */
    val frame: DataFrame = ds.toDF()

    spark.close()
  }

}

case class user(name:String, age:Int)

