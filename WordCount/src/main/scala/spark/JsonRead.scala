package spark

import org.apache.spark.{SparkConf, SparkContext}
import scala.util.parsing.json.JSON

/**
  *
  */
object JsonRead {
  def main(args: Array[String]) {
    val inputFile = "/user/hdfs/people.json"
    val conf = new SparkConf().setAppName("JsonRead").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val jsonStrs = sc.textFile(inputFile)
    val result = jsonStrs.map(s => JSON.parseFull(s))
    result.foreach({ r =>
      r match {
        case Some(map: Map[String, Any]) => println(map)
        case None => println("Parsing failed")
        case other => println("Unknown data structure: " + other)
      }
    }
    )

  }
}
