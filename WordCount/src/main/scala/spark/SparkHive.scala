package spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}
import spark.SparkHive.spark


/**
  * * @description: ${DESCRIPTION}
  * * @author wuxiaopeng
  * * @date 2019/9/10 16:30
  * *
  */
object SparkHive {

  def main(args: Array[String]): Unit = {
    findStudentInfo()
  }

  def findStudentInfo(): Unit ={
    val spark = SparkSession
      .builder()
      .appName("spark sql exmaple")
      .config("hive.metastore.uris", "thrift://node0.example.com:9083")
      .config("spark.sql.warehouse.dir", "/apps/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate()
    spark.sql("use sparktest")
    val sql="select * from sparktest.student"
    val data=spark.sql(sql)
    data.show();
    //    val warehouseLocation = "spark-warehouse"
//    val spark = SparkSession.builder().appName("Spark Hive Example").config("spark.sql.warehouse.dir", warehouseLocation).enableHiveSupport().getOrCreate()
//    spark.sql("SELECT * FROM sparktest.student").show()
  }

  case class Record(key: Int, value: String)
  val conf = new SparkConf().setAppName("SparkSql").setMaster("local[2]")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)
  // 在Scala中使用反射方式，进行RDD到DataFrame的转换，需要手动导入一个隐式转换
  import sqlContext.implicits._
  import spark.sql
  val spark = SparkSession.builder().getOrCreate()
}
