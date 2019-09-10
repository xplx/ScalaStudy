# ScalaStudy
用于学习大数据spark
学习网址：
http://dblab.xmu.edu.cn/blog/spark/

异常：org.apache.spark.SparkException: A master URL must be set in your configuration
解决办法：在IDE中点击Run -> Edit Configuration，在右侧VM options中输入“-Dspark.master=local”，指示本程序本地单线程运行


{"name":"Michael","age":40}
{"name":"Andy", "age":30}
{"name":"Justin", "age":19}


Name: 40  favorite color:Michael
Name: 30  favorite color:Andy
Name: 19  favorite color:Justin