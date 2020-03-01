import java.io.{BufferedReader, File, FileReader}


class IncreaseFile(file:File){

  def count():Int={
    var sum = 0
    val reader = new FileReader(file)
    val br = new BufferedReader(reader)
    var line = br.readLine()
    while (line!=null){
      sum += 1
      line = br.readLine()
    }
    sum
  }
}

/**
 * File没有count方法，
 * 利用隐式转换
 *
 */
object demo {
implicit def fileAdd(file: File)=new IncreaseFile(file)

  def main(args: Array[String]): Unit = {

    val file: File = new File("E:\\test.txt")
    println(s"Count:${file.count()}")

  }

}
