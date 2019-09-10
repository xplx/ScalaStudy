package spark

import java.util.Properties

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext, SparkSession}

/**
  * * @description: 连接mysql操作
  * * @author wuxiaopeng
  * * @date 2019/9/10 15:59
  */
object SparkMysql {
  def main(args: Array[String]): Unit = {
    listStudent()
  }
  def saveStudent(): Unit ={
    //下面我们设置两条数据表示两个学生信息
    val studentRDD = spark.sparkContext.parallelize(Array("3 Rongcheng M 26","4 Guanhua M 27")).map(_.split(" "))

    //下面要设置模式信息
    val schema = StructType(List(StructField("id", IntegerType, true),
      StructField("name", StringType, true),StructField("gender", StringType, true),StructField("age", IntegerType, true)))

    //下面创建Row对象，每个Row对象都是rowRDD中的一行
    val rowRDD = studentRDD.map(p => Row(p(0).toInt, p(1).trim, p(2).trim, p(3).toInt))
    //建立起Row对象和模式之间的对应关系，也就是把数据和模式对应起来
    val studentDF = spark.createDataFrame(rowRDD, schema)

    //下面创建一个prop变量用来保存JDBC连接参数
    val prop = new Properties()
    prop.put("user", "ambari")
    prop.put("password", "123")
    prop.put("driver","com.mysql.jdbc.Driver")
    studentDF.write.mode("append").jdbc("jdbc:mysql://192.168.137.134:3306/spark", "spark.student", prop)
  }

  /**
    * 获取student表数据
    */
  def listStudent(): Unit ={
    val jdbcDF = spark.read.format("jdbc").
      option("url", "jdbc:mysql://192.168.137.134:3306/spark").
      option("driver","com.mysql.jdbc.Driver").
      option("dbtable", "student").
      option("user", "ambari").
      option("password", "123").load()
    jdbcDF.show()
  }

  val conf = new SparkConf().setAppName("SparkSql").setMaster("local[2]")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)
  // 在Scala中使用反射方式，进行RDD到DataFrame的转换，需要手动导入一个隐式转换
  import sqlContext.implicits._
  val spark = SparkSession.builder().getOrCreate()
}
