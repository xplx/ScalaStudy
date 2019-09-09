package spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @description:
  * @author wuxiaopeng
  * @date 2019/9/9 11:15
  */
object WordCount extends Serializable {
  def main(args: Array[String]): Unit = {
    countSell()
  }

  def wordHdfs(): Unit = {
    //这里默认获取hdfs文件
    val inputFile = "/user/hdfs/word.txt"
    val conf = new SparkConf().setAppName("WordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile(inputFile)
    //转换操作,整个转换过程只是记录了转换的轨迹，并不会发生真正的计算。
    val wordCount = textFile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey((a, b) => a + b)
    //执行过程打印结果
    wordCount.foreach(println)
    //将结果保存hdfs
    wordCount.saveAsTextFile("/user/hdfs/writeback")
  }

  /**
    *
    */
  def wordMap(): Unit = {
    val inputFile = "/user/hdfs/word.txt"
    val conf = new SparkConf().setAppName("WordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)
    //通过并行集合（数组）创建RDD
    val rdd = sc.parallelize(inputFile)
    val pairRDD = rdd.map(word => (word, 1))
    pairRDD.foreach(println)
  }

  /**
    * 常用的键值对转换操作:reduceByKey()、groupByKey()、sortByKey()、join()、cogroup()
    * reduceByKey(func):使用func函数合并具有相同键的值。
    * groupByKey(func):对具有相同键的值进行分组。
    * sortByKey():返回一个根据键排序的RDD。
    * mapValues(func):对键值对RDD中的每个value都应用一个函数，但是，key不会发生变化。
    */
  def rddMap(): Unit = {
    val list = List("Hadoop", "Spark", "Hive", "Spark")
    val conf = new SparkConf().setAppName("WordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(list)
    val pairRDD = rdd.map(word => (word, 1))
    //pairRDD.reduceByKey((a,b)=>a+b).foreach(println)
    //pairRDD.groupByKey().foreach(println)
    //keys只会把键值对RDD中的key返回形成一个新的RDD
    //pairRDD.keys.foreach(println)
    //values只会把键值对RDD中的value返回形成一个新的RDD。
    //pairRDD.values.foreach(println)
    // pairRDD.sortByKey().foreach(println)
    pairRDD.mapValues(x => x * 10).foreach(println)
  }

  /**
    * join(连接)操作是键值对常用的操作。
    * 包括内连接(join)、左外连接(leftOuterJoin)、右外连接(rightOuterJoin)等。
    * 对于给定的两个输入数据集(K,V1)和(K,V2)，只有在两个数据集中都存在的key才会被输出，最终得到一个(K,(V1,V2))类型的数据集。
    */
  def rddJoin(): Unit ={
    val conf = new SparkConf().setAppName("WordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val pairRDD1 = sc.parallelize(Array(("spark",1),("spark",2),("hadoop",3),("hadoop",5)))
    val pairRDD2 = sc.parallelize(Array(("spark","fast")))
    pairRDD1.join(pairRDD2).foreach(println)
  }

  def countSell(): Unit ={
    val conf = new SparkConf().setAppName("WordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)
    //调用parallelize()方法生成RDD,将一个存在的集合，变成一个RDD.
    val rdd = sc.parallelize(Array(("spark",2),("hadoop",6),("hadoop",4),("spark",6)))
    //value转换成键值对(value,1)(hadoop,(4,1))
    val mapValue = rdd.mapValues(x=>(x,1))
    mapValue.foreach(println)
    //
    val byKey = mapValue.reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))
    byKey.foreach(println)
    val resultValue = byKey.mapValues(x=>(x._1/x._2)).collect()
    for (elem <- resultValue) {
      println(elem)
    }
  }

  /**
    * 统计词数量
    */
  def rddCount(): Unit = {
    val lines = baseTextFileSc()
    //lines.filter()会遍历lines中的每行文本
    val pairRDD = lines.filter(line => line.contains("i")).count()
    println(pairRDD)
  }

  /**
    * 查找单词最大数量
    */
  def rddWorldMax(): Unit = {
    val lines = baseTextFileSc()
    // lines.map()，是一个转换操作,map(func)：将每个元素传递到函数func中，并将结果返回为一个新的数据集.
    // line.split(” “).size这个处理逻辑的功能是，对line文本内容进行单词切分，得到很多个单词构成的集合.
    // 是一个整型RDD，里面每个元素都是整数值（也就是单词的个数）。
    val map = lines.map(line => line.split(" ").size)
    //对a和b执行大小判断，保留较大者3.依此类推。
    val pairRDD = map.reduce((a, b) => if (a > b) a else b)
    println(pairRDD)
  }

  /**
    * rdd持久化计算
    */
  def rddPersist(): Unit = {
    val list = List("Hadoop", "Spark", "Hive")
    val conf = new SparkConf().setAppName("WordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(list)
    rdd.cache()
    println(rdd.count())
    println(rdd.collect().mkString(","))
  }

  /**
    * 定义公共函数
    *
    * @return
    */
  def baseTextFileSc(): RDD[String] = {
    val inputFile = "/user/hdfs/word.txt"
    val conf = new SparkConf().setAppName("WordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)
    //通过并行集合（数组）创建RDD
    val lines = sc.textFile(inputFile)
    return lines
  }
}
