package com.scala

/**
 * 计算100-999的所有的水仙花数
 * 水仙花数是一个三位数其各位数字的立方和等于该数本身 eg：123
 */
object Flower {

  def util(data:Int): Unit ={
    var a = data / 100
    var b = data /10 % 10
    var c = data % 10
    var sum = (Math.pow(a,3) + Math.pow(b,3) + Math.pow(c,3))
    if(sum == data)
      println(sum.toInt)
  }

  def main(args: Array[String]): Unit = {
    for(i <- 100 until 1000)
      util(i)

  }
}
