package com.scala

/**
 * 分别用方法和函数两种方式来实现求1+2+3+。。。+n的和，n是参数。定义好调用方法和函数
 */
object Add_To_N {

  //方法
  def add2N_1(end: Int) = {

    var sum: Int = 0

    for (i <- 1 to end)
      sum += i
    println(sum)
  }

  // 函数
  var add2N_2 = (end: Int) => {

    var sum: Int = 0

    for (i <- 1 to end)
      sum += i
    println(sum)
  }

  def main(args: Array[String]): Unit = {
    add2N_1(4)
    add2N_2(4)
  }
}
