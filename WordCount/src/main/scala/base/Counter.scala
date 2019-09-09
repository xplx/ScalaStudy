package base

/**
  * 类是用来创建对象的蓝图
  */
class Counter {
  //
  private var value = 0
  //如果字段前面什么修饰符都没有，就默认是public,Unit无返回值
  def increment(): Unit = { value += 1}
  def current(): Int = {value}
}

object MyCounter{
  def main(args: Array[String]): Unit = {
    //new 一个对象访问类里面函数
    val myCounter = new Counter
    myCounter.increment()
    println(myCounter.current)
  }
}