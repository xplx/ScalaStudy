package base



object LoopTest {
  def main(args: Array[String]): Unit = {
    val list = List(0,1,3,4)
    for (elam <- list){
      println(elam)
    }

    //不可变映射
    val university = Map("XMU" -> "Xiamen University", "THU" -> "Tsinghua University","PKU"->"Peking University")
    //打印key和value
    for ((k,v) <- university) printf("Code is : %s and name is: %s\n",k,v)
    //打印k
    for (k<-university.keys) println(k)
    //打印v
    for (v <- university.values){
      println(v)
    }

  }
}
