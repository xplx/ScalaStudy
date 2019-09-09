package base

/**
  * Scala的模式匹配最常用于match语句中。
  */
object MatchTest {
  def main(args: Array[String]): Unit = {
    //匹配值
    val colorNum = 2
    val colorStr = colorNum match {
      case 1 => "red"
      case 2 => "green"
      case 3 => "yellow"
      case _ => "Not Allowed"
    }
    println(colorStr)
  }
}
