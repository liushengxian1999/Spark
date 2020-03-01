package com.scala

object Odd_Even {

  def main(args: Array[String]): Unit = {
    var odd = 0
    var even = 0

    for (num <- 1 to 100){
      if (0==num%2)
        even+=num
      else
        odd+=num
    }
    println(s"奇数：${odd} \n偶数：${even}")


  }
}
