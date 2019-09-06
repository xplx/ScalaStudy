import org.apache.spark.{SparkConf, SparkContext}

object WordCount extends Serializable {
  def main(args: Array[String]): Unit = {
    //这里默认获取hdfs文件
    val inputFile =  "/user/hdfs/word.txt"
    val conf = new SparkConf().setAppName("WordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile(inputFile)
    val wordCount = textFile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey((a, b) => a + b)
    //执行过程打印结果
    wordCount.foreach(println)
    //将结果保存hdfs
    wordCount.saveAsTextFile("/user/hdfs/writeback")
  }
}
