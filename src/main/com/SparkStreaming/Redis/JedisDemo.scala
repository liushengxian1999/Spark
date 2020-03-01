package com.SparkStreaming.Redis

import redis.clients.jedis.Jedis

object JedisDemo {

  def main(args: Array[String]): Unit = {


    // 连接
    val con = new Jedis("h1", 6379)

    //建立key->liu value->1
    con.set("liu","1")
    //增加1
    con.incr("liu")
    //增加指定
    con.incrBy("liu",100)
    // 获得key的值
    val num = con.get("liu")
    println(num)
  }

}
