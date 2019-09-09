package base


object ArrayTest {
  def main(args: Array[String]): Unit = {
    setFunction()
  }

  /**
    * 数组声明
    */
  def arrayFunction(): Unit ={
    //声明一个长度为3的字符串数组，每个数组元素初始化为null
    val myStrArr = new Array[String](3)
    myStrArr(0) = "BigData"
    myStrArr(1) = "Hadoop"
    myStrArr(2) = "Spark"
    for (i <- 0 to 2){
      println(myStrArr(i))
    }

    //元组
    val tuple = ("BigData",2015,45.0)
    for (j <- 0 to 2) {
      println(tuple._1)
    }
  }
  /**
    * 集(set)是不重复元素的集合。
    */
  def setFunction(){
    var mySet = Set("Hadoop","Spark")
    //添加值
    mySet += "Scala"
    println(mySet.contains("Scala"))
  }
}
