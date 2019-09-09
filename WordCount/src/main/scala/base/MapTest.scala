package base


object MapTest {
  def main(args: Array[String]): Unit = {
    val books = List("Hadoop", "Hive", "HDFS")
    books.map(s => s.toUpperCase)
    for (elem <- books) {
      println(elem)
    }

    books flatMap (s => s.toList)
    for (s <- books) {
      println(s)
    }
  }
}
