package base

object ReduceTest {
  def main(args: Array[String]): Unit = {
    val list = List(1,2,3,4,5)
    println(list.reduceLeft(_ + _))
    /**
      * reduceLeft(_ + _)表示从列表头部开始，对两两元素进行求和操作，
      * 下划线是占位符，用来表示当前获取的两个元素，两个下划线之间的是
      * 操作符，表示对两个元素进行的操作，这里是加法操作
      */
    println(list.reduceRight(_ + _))
  }
}
