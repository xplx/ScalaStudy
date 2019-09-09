package base



object FilterTest {
  def main(args: Array[String]): Unit = {
    val university = Map("XMU" -> "Xiamen University", "THU" -> "Tsinghua University",
      "PKU"->"Peking University","XMUT"->"Xiamen University of Technology")
    //过滤值里面有Xiamen
    val universityOfXiamen = university filter {kv => kv._2 contains "Xiamen"}
    println(universityOfXiamen)
    universityOfXiamen foreach {kv => println(kv._1+":"+kv._2)}
  }
}
