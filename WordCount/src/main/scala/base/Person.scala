package base



object Person {
  /**
    * 单例对象
    */
  private var lastId = 0  //一个人的身份编号
  def newPersonId() = {
    lastId +=1
    lastId
  }

  def main(args: Array[String]): Unit = {
    printf("The first person id is %d.\n",Person.newPersonId())
    printf("The second person id is %d.\n",Person.newPersonId())
    printf("The third person id is %d.\n",Person.newPersonId())
  }
}
