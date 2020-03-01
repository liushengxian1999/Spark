package IP

import scala.io.{BufferedSource, Source}

object IP_Along {

  /**
   * 单机模式
   * 1、整理ip.txt文件  转换成（开始ip，结束ip，省份）
   * 2、把IP转换成十进制 方便查找
   * 3、用二分法查找相应数据的下标
   */

    //整理IP信息 (开始，结束，省份)
    def SetIp(path:String): Array[(Long, Long, String)]={

//      val bf: BufferedSource = Source.fromFile("D:\\Spark_Test\\IP\\data\\ip.txt")
      // 读文件
      var bf = Source.fromFile(path)
      val lines: Iterator[String] = bf.getLines()

      val rules: Array[(Long, Long, String)] = lines.map(x => {

        val line = x.split("\\|")
        val start: Long = line(2).toLong
        val end: Long = line(3).toLong
        val sheng = line(6)

        (start, end, sheng)
      }).toArray
      rules
  }


  // IP转成十进制
  def IPChange(ip:String):Long={

    val word = ip.split("\\.")
    var sum: Long = 0
    for (i <- 0 until 4) {

      val value = Math.pow(256, 3 - i)
      sum += word(i).toLong * value.toLong
    }
    sum
  }

  // 二分法查找
  def binSearch(rules:Array[(Long, Long, String)],ip:Long):Int = {

    var left = 0
    var right = rules.length-1

    while (left <= right){

      var mid = (left+right)/2
      if(ip >= rules(mid)._1 && ip <= rules(mid)._2){
        return mid
      }else if(ip < rules(mid)._1){
        right = mid - 1
      }else{
        left = mid + 1
      }
    }
    -1
  }

  def main(args: Array[String]): Unit = {

    // 获得规则
    val rules: Array[(Long, Long, String)] = SetIp("D:\\Spark_Test\\IP\\data\\ip.txt")
    // IP信息
    var ip = "125.213.100.123"
    val IP = IPChange(ip)
    // 在规则中查找 返回下标
    val index: Int = binSearch(rules,IP )
    if(index != -1){
      val data: (Long, Long, String) = rules(index)
      println(data._3)
    }




  }
}
