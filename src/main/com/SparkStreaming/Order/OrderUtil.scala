package com.SparkStreaming.Order

import IP.MyUtil
import com.SparkStreaming.Redis.Jedis
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

object OrderUtil {

  /**
   *  计算成交量总额
   */
  def TotalPrice(fields: RDD[Array[String]]) = {

    // 获得price
    val price: RDD[Double] = fields.map(_(4).toDouble)

    //将该批次价格累加
    val sum: Double = price.reduce(_ + _)

    // 存到Redis
    val con = Jedis.getConnection()

    //将历史值和当前的值进行累加
    con.incrByFloat("TotalPrice",sum)
    con.close()
  }

  /**
   * 计算每个省的成交金额
   */
  def ProvinceAndPrice(ref:Broadcast[Array[(Long, Long, String)]],fields: RDD[Array[String]])={

    val provinceAndPrice: RDD[(String, Double)] = fields.map(x => {

      // 获得价钱
      var price = x(4).toDouble
      //获得IP
      var ip = x(1)
      //IP转换十进制
      val IP: Long = MyUtil.IP2Ten(ip)
      //获得规则
      val rules: Array[(Long, Long, String)] = ref.value
      //查找IP所在的省份
      val index = MyUtil.binSearch(rules, IP)

      var province = "未知"
      if (index != -1) {
       var province = rules(index)._3
      }

      (province, price)
    })

     // 聚合每个省份的总金额
    val result: RDD[(String, Double)] = provinceAndPrice.reduceByKey(_ + _)
    //写入Redis
    result.foreachPartition(par =>{

      par.foreach(x =>{
        val con = Jedis.getConnection()
        con.incrByFloat(x._1,x._2)
        con.close()
      })
    })
  }

  /**
   * 计算商品分类的总金额
   */
  def Product(fields: RDD[Array[String]]) = {

    //整理数据
    //对field的map方法是在哪一端调用的呢？Driver
    val result: RDD[(String, Double)] = fields.map(x => {

      // 商品的分类
      val product: String = x(2)
      //  商品的价格
      val price = x(4).toDouble

      (product, price)
    })

    // 聚合
    val reduced: RDD[(String, Double)] = result.reduceByKey(_ + _)
    // 存入Redis
    //在Driver端拿Jedis连接不好
    reduced.foreachPartition(x =>{

      //获取一个Jedis连接
      //这个连接其实是在Executor中的获取的
      val con = Jedis.getConnection()

      x.foreach(i =>{
        con.incrByFloat(i._1,i._2)
      })
      con.close()
    })
  }
}
