package com.scala

/**
 * 定义常量PI的值为3.1415926
 * 定义一个圆的半径r为2
 * 计算圆的周长C及面积S
 */
object PI {

  // PI
  val PI = 3.1415926

  // flag为C,计算周长；flag为S，计算面积
  def area(r: Int, flag: String) = flag match {
    case ("C") => println(s"周长：${PI * 2 * r}")
    case ("S") => println(s"面积：${PI * r * r}")
    case _ => print("输入错误")
  }

  def main(args: Array[String]): Unit = {
    var r = 2
    // 计算周长
    area(r, "C")
    //计算面积
    area(r, "S")
  }
}
