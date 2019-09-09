package base



class TestApplyClass {
  def apply(param: String): String = {
    println("apply method called, parameter is: " + param)
    "Hello World!"
  }
}

/**
  * 伴生类和伴生对象中的apply方法实例
  */
class ApplyTest{
  def apply() = println("apply method in class is called!")
  def greetingOfClass: Unit ={
    println("Greeting method in class is called.")
  }
}

object ApplyTest{
  def apply() = {
    println("apply method in object is called")
    new ApplyTest()
  }
}

object TestApplyObject{
  def main(args: Array[String]): Unit = {
    //这里会调用伴生对象中的apply方法
    val a = ApplyTest()
    a.greetingOfClass
    // 这里会调用伴生类中的apply方法
    a()
  }
}
