package spark

import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client.{Get, HBaseAdmin, HTable, Put}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}


object SparkOperateHBase {
  def main(args: Array[String]) {
    saveData()
  }

  def getStudentInfo(): Unit ={
    //创建Hbase配置
    val conf = HBaseConfiguration.create()
    //spark配置
    val sparkConf = new SparkConf().setAppName("SparkOperateHBase").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    //设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE, "student")
    val stuRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    val count = stuRDD.count()
    println("Students RDD Count:" + count)
    //放入缓存
    stuRDD.cache()

    //遍历输出
    stuRDD.foreach({ case (_,result) =>
      val key = Bytes.toString(result.getRow)
      val name = Bytes.toString(result.getValue("info".getBytes,"name".getBytes))
      val gender = Bytes.toString(result.getValue("info".getBytes,"gender".getBytes))
      val age = Bytes.toString(result.getValue("info".getBytes,"age".getBytes))
      println("Row key:"+key+" Name:"+name+" Gender:"+gender+" Age:"+age)
    })
  }

  /**
    * 创建表
    */
  def createTable(): Unit ={
    val table_name = "test1"
    val conf = HBaseConfiguration.create()
    val admin = new HBaseAdmin(conf)
    if (admin.tableExists(table_name))
    {
      admin.disableTable(table_name)
      admin.deleteTable(table_name)
    }
    val htd = new HTableDescriptor(table_name)
    //列族
    val hcd = new HColumnDescriptor("id")
    //add  column to table
    htd.addFamily(hcd)
    admin.createTable(htd)
    println("success")
  }

  /**
    * 保存数据
    */
  def saveData(): Unit ={
    val table_name = "test1"
    val conf = HBaseConfiguration.create()
    val htd = new HTableDescriptor(table_name)
    //put data to HBase table
    val tableName = htd.getName
    val table = new HTable(conf, tableName)
    val dataBytes = Bytes.toBytes("id")
    for (c <- 1 to 10) {
      val row = Bytes.toBytes("row" + c.toString)
      val p1 = new Put(row)
      p1.add(dataBytes, Bytes.toBytes(c.toString), Bytes.toBytes("value" + c.toString))
      table.put(p1)
    }

    for (c <- 1 to 10) {
      val g = new Get(Bytes.toBytes("row" + c.toString))
      println("Get:" + table.get(g))
    }
  }
}
