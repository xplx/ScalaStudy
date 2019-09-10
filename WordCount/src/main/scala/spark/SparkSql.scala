package spark

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}



/**
  * * @description: DataFrame数据框架让Spark具备了处理大规模结构化数据的能力，
  * 不仅比原有的RDD转化方式更加简单易用，
  * 而且获得了更高的计算性能。Spark能够轻松实现从MySQL到DataFrame的转化，并且支持SQL查询。
  * * @author wuxiaopeng
  * * @date 2019/9/10 9:51
  */
object SparkSql {
  def main(args: Array[String]): Unit = {
    changeDataFormat()
  }

  val conf = new SparkConf().setAppName("SparkSql").setMaster("local[2]")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)
  // 在Scala中使用反射方式，进行RDD到DataFrame的转换，需要手动导入一个隐式转换
  import sqlContext.implicits._

  //放在object外面
  case class Person(name: String, age: Long)

  /**
    * rdd数据转DataFrame数据
    * 在利用反射机制推断RDD模式时，需要首先定义一个case class，
    * 因为，只有case class才能被Spark隐式地转换为DataFrame。
    */
  def rddToDataFramePerson(): Unit ={
    //定义一个case class,因为，只有case class才能被Spark隐式地转换为DataFrame。
    val spark = SparkSession.builder().getOrCreate()
    val peopleSplit = spark.sparkContext.textFile("/user/hdfs/people.txt").map(_.split(","))
    val peopleMap = peopleSplit.map(attributes => Person(attributes(0), attributes(1).trim.toInt))
    val peopleDF = peopleMap.toDF()

    //必须注册为临时表才能供下面的查询使用
    peopleDF.createOrReplaceTempView("people")
    //最终生成一个DataFrame
    val personsRDD = spark.sql("select name,age from people where age > 20")
    personsRDD.map(t => "Name:"+t(0)+","+"Age:"+t(1)).show()
  }

  /**
    * 当无法提前定义case class时，就需要采用编程方式定义RDD模式。
    */
  def rddToDataFrame(): Unit ={
    val spark = SparkSession.builder().getOrCreate()
    //生成 RDD
    val peopleRDD = spark.sparkContext.textFile("/user/hdfs/people.txt")
    val rowRDD = peopleRDD.map(_.split(",")).map(attributes => Row(attributes(0), attributes(1).trim))

    //定义一个模式字符串//定义一个模式字符串
    val schemaString = "name age"
    //根据模式字符串生成模式
    val fields = schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, nullable = true))
    //从上面信息可以看出，schema描述了模式信息，模式中包含name和age两个字段
    val schema = StructType(fields)

    val peopleDF = spark.createDataFrame(rowRDD, schema)

    //必须注册为临时表才能供下面查询使用
    peopleDF.createOrReplaceTempView("people")
    val results = spark.sql("SELECT name,age FROM people")
    results.map(attributes => "name: " + attributes(0)+","+"age:"+attributes(1)).show()


  }

  /**
    * 数据格式转换
    */
  def changeDataFormat(): Unit ={
    val spark = SparkSession.builder().getOrCreate()
    //载入数据
    val peopleDF = spark.read.format("json").load("/user/hdfs/people.json")

    //保存数据
    //peopleDF.select("name", "age").write.format("csv").save("/user/hdfs/newpeople.csv")
    //文本格式
    peopleDF.rdd.saveAsTextFile("/user/hdfs/newpeople..txt")

    //读取保存数据
    val textFile = sc.textFile("/user/hdfs/newpeople.csv")
    textFile.foreach(println)
  }

  /**
    * 查询json数据
    */
  def findJson(): Unit ={
    val spark = SparkSession.builder().getOrCreate()
    val df = spark.read.json("/user/hdfs/people.json")
    df.cache()
    df.show()
    // 打印模式信息
    df.printSchema()
    // 选择多列
    df.select(df("name"),df("age")+1).show()
    // 条件过滤
    df.filter(df("age") > 20 ).show()
    // 分组聚合
    df.groupBy("age").count().show()
    // 排序
    df.sort(df("age").desc).show()
    //多列排序
    df.sort(df("age").desc, df("name").asc).show()
    //对列进行重命名
    df.select(df("name").as("username"),df("age")).show()
  }
}
