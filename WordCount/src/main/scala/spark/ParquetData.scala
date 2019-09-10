package spark

import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}


/**
  * * @description: ${DESCRIPTION}
  * * @author wuxiaopeng
  * * @date 2019/9/10 15:38
  * *
  */
object ParquetData {
  def main(args: Array[String]): Unit = {
    readParquetData()
  }

  def readParquetData(): Unit ={
    val parquetFileDF = spark.read.parquet("/user/hdfs/newpeople.parquet")

    //创建临时表
    parquetFileDF.createOrReplaceTempView("parquetFile")
    val namesDF = spark.sql("SELECT * FROM parquetFile")
    namesDF.foreach(attributes =>println("Name: " + attributes(0)+"  favorite color:"+attributes(1)))
  }

  def saveParquetData(): Unit ={
    val parquetFileDF = spark.read.json("/user/hdfs/people.json")

    //创建临时表
    parquetFileDF.write.parquet("/user/hdfs/newpeople.parquet")
  }

  val conf = new SparkConf().setAppName("SparkSql").setMaster("local[2]")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)
  // 在Scala中使用反射方式，进行RDD到DataFrame的转换，需要手动导入一个隐式转换
  import sqlContext.implicits._
  val spark = SparkSession.builder().getOrCreate()
}
