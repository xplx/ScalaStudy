package spark

import java.io.File

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import spark.SparkMysql.sc
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


  def findStudentInfo(): Unit = {
    import spark.implicits._
    import spark.sql
    sql("select * from sparktest.student").show()
  }

  def save(): Unit = {
    import spark.sql

    //下面我们设置两条数据表示两个学生信息
    val studentRDD = spark.sparkContext.parallelize(Array("3 Rongcheng M 26", "4 Guanhua M 27")).map(_.split(" "))

    //下面要设置模式信息
    val schema = StructType(List(StructField("id", IntegerType, true),
      StructField("name", StringType, true),
      StructField("gender", StringType, true),
      StructField("age", IntegerType, true)))
    //下面创建Row对象，每个Row对象都是rowRDD中的一行
    val rowRDD = studentRDD.map(p => Row(p(0).toInt, p(1).trim, p(2).trim, p(3).toInt))
    //建立起Row对象和模式之间的对应关系，也就是把数据和模式对应起来
    val studentDF = spark.createDataFrame(rowRDD, schema)

    studentDF.show()
    //下面注册临时表
    studentDF.registerTempTable("tempTable")
    //插入数据
    sql("insert into sparktest.student select * from tempTable")

  }


  val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
    .setMaster("local[2]")

  val spark = SparkSession.builder()
    .config(conf)
    // 指定hive的metastore的端口  默认为9083 在hive-site.xml中查看
    .config("hive.metastore.uris", "thrift://node0.example.com:9083")
    //指定hive的warehouse目录
    .config("spark.sql.warehouse.dir", "hdfs://node0.example.com:8020/apps/hive/warehouse")
    //直接连接hive
    .enableHiveSupport()
    .getOrCreate()
}
