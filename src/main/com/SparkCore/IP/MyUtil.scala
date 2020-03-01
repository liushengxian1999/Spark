package IP

import java.sql.DriverManager

import scala.io.Source

object MyUtil {


  // 设置IP信息规则 （传入一个字符串， 返回（开始IP,结束IP,省份））
  def  SetRules(path:String): Array[(Long, Long, String)]={

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

  // IP转成十进制(传入一个IP 返回十进制)
  def IP2Ten(ip:String):Long={

    val word = ip.split("\\.")
    var sum: Long = 0
    for (i <- 0 until 4) {

      val value = Math.pow(256, 3 - i)
      sum += word(i).toLong * value.toLong
    }
    sum
  }


  // 二分法查找 （传入IP规则，IP） 返回找到IP信息的下标
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

  def down2mysql(x:Iterator[(String,Int)]): Unit ={

    var url = "jdbc:mysql://localhost:3306/bigdata"
    val con = DriverManager.getConnection(url,"root","123")
    val pst = con.prepareStatement("insert into IP values (?,?)")

    x.foreach(d =>{

      pst.setString(1,d._1)
      pst.setInt(2,d._2)
      pst.executeUpdate()
    })
    pst.close()
    con.close()
  }


}
