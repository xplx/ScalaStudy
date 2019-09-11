package spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel, HashingTF, IDF, Tokenizer, Word2Vec}
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.Row


/**
  * 机器学习工作流:
  * * @description: PipeLine：翻译为工作流或者管道。
  * 工作流将多个工作流阶段（转换器和估计器）连接在一起，形成机器学习的工作流，并获得结果输出。
  * 要构建一个 Pipeline工作流，首先需要定义 Pipeline 中的各个工作流阶段PipelineStage，（包括转换器和评估器），
  * 比如指标提取和转换模型训练等。有了这些处理特定问题的转换器和 评估器，
  * 就可以按照具体的处理逻辑有序的组织PipelineStages 并创建一个Pipeline。比如：
  * 通常会包含源数据ETL（抽取、转化、加载）
  * * @author wuxiaopeng
  * * @date 2019/9/11 9:11
  */
object SparkMlStudy {
  def main(args: Array[String]): Unit = {
    CountVectorizerDate
  }

  /**
    * CountVectorizer旨在通过计数来将一个文档转换为向量。
    * 当不存在先验字典时，Countvectorizer作为Estimator提取词汇进行训练，
    * 并生成一个CountVectorizerModel用于存储相应的词汇向量空间。
    * 该模型产生文档关于词语的稀疏表示，其表示可以传递给其他算法，例如LDA。
    */
  def CountVectorizerDate(): Unit = {
    //其包含id和words两列，可以看成是一个包含两个文档的迷你语料库。
    val df = spark.createDataFrame(Seq(
      (0, Array("a", "b", "c")),
      (1, Array("a", "b", "b", "c", "a"))
    )).toDF("id", "words")

    //这里设定词汇表的最大量为3，设定词汇表中的词至少要在2个文档中出现过，以过滤那些偶然出现的词汇。
    val cvModel: CountVectorizerModel = new CountVectorizer().
      setInputCol("words").
      setOutputCol("features").
      setVocabSize(3).
      setMinDF(2).
      fit(df)
    //以通过CountVectorizerModel的vocabulary成员获得到模型的词汇表：
    cvModel.vocabulary
    cvModel.transform(df).show(false)
    val cvm = new CountVectorizerModel(Array("a", "b", "c")).
      setInputCol("words").
      setOutputCol("features")
    cvm.transform(df).select("features").show(false)
  }

  /**
    * Word2Vec 是一种著名的 词嵌入（Word Embedding） 方法，它可以计算每个单词在其给定语料库环境下的
    * 分布式词向量（Distributed Representation，亦直接被称为词向量）。
    * 词向量表示可以在一定程度上刻画每个单词的语义。
    */
  def Word2VecData(): Unit = {
    val documentDF = spark.createDataFrame(Seq(
      "Hi I heard about Spark".split(" "),
      "I wish Java could use case classes".split(" "),
      "Logistic regression models are neat".split(" ")
    ).map(Tuple1.apply)).toDF("text")

    val test = spark.createDataFrame(Seq(
      "Hi I ".split(" "),
      "I wish ".split(" "),
      "Logistic".split(" ")
    ).map(Tuple1.apply)).toDF("text")

    val word2Vec = new Word2Vec().
      setInputCol("text").
      setOutputCol("result").
      setVectorSize(3).
      setMinCount(0)

    val model = word2Vec.fit(documentDF)

    val result = model.transform(test)

    result.select("result").take(3).foreach(println)
  }

  /**
    * TF-IDF (HashingTF and IDF)
    * ​ “词频－逆向文件频率”（TF-IDF）是一种在文本挖掘中广泛使用的特征向量化方法，
    * 它可以体现一个文档中词语在语料库中的重要程度。
    */
  def TFIDFData(): Unit = {
    //文档集合
    val sentenceData = spark.createDataFrame(Seq(
      (0, "I heard about Spark and I love Spark"),
      (0, "I wish Java could use case classes"),
      (1, "Logistic regression models are neat"),
      (1, "Hello world")
    )).toDF("label", "sentence")

    //即可用tokenizer对句子进行分词
    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
    val wordsData = tokenizer.transform(sentenceData)
    wordsData.show(false)

    //得到分词后的文档序列后，即可使用HashingTF的transform()方法把句子哈希成特征向量，这里设置哈希表的桶数为2000。
    val hashingTF = new HashingTF().
      setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(2000)
    val featurizedData = hashingTF.transform(wordsData)
    //分词序列被变换成一个稀疏特征向量，其中每个单词都被散列成了一个不同的索引值，特征向量在某一维度上的值即该词汇在文档中出现的次数。
    featurizedData.select("rawFeatures").show(false)

    //使用IDF来对单纯的词频特征向量进行修正，使其更能体现不同词汇对文本的区别能力，
    // IDF是一个Estimator，调用fit()方法并将词频向量传入，即产生一个IDFModel。
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)
    val rescaledData = idfModel.transform(featurizedData)

    rescaledData.select("features", "label").take(3).foreach(println)
  }

  def trainingData(): Unit = {
    //第一步：引入要包含的包并构建训练数据集
    val training = spark.createDataFrame(Seq(
      (0L, "a b c d e spark", 1.0),
      (1L, "b d", 0.0),
      (2L, "spark f g h", 1.0),
      (3L, "hadoop mapreduce", 0.0))).toDF("id", "text", "label")

    //测试数据
    val test = spark.createDataFrame(Seq(
      (4L, "a b c d e spark"),
      (5L, "l m n"),
      (6L, "spark b"),
      (7L, "apache hadoop"),
      (8L, "spark d"),
      (9L, "spark a"),
      (10L, "spark c"),
      (11L, "spark f")
    )).toDF("id", "text")

    // 前两个（Tokenizer和HashingTF）是Transformers（转换器）
    val tokenizer = new Tokenizer().
      setInputCol("text").
      setOutputCol("words")

    val hashingTF = new HashingTF().
      setNumFeatures(1000).
      setInputCol(tokenizer.getOutputCol).
      setOutputCol("features")

    //翻译成估计器或评估器，它是学习算法或在训练数据上的训练方法的概念抽象。
    val lr = new LogisticRegression().
      setMaxIter(10).
      setRegParam(0.01)

    //有了这些处理特定问题的转换器和评估器，接下来就可以按照具体的
    // 处理逻辑有序的组织PipelineStages 并创建一个Pipeline。
    // 翻译为工作流或者管道。工作流将多个工作流阶段（转换器和估计器）连接在一起，形成机器学习的工作流，并获得结果输出。
    val pipeline = new Pipeline().setStages(Array(tokenizer, hashingTF, lr))

    //现在构建的Pipeline本质上是一个Estimator，在它的fit（）方法运行之后，
    // 它将产生一个PipelineModel，它是一个Transformer。
    val model = pipeline.fit(training)

    model.transform(test).
      select("id", "text", "probability", "prediction").collect().
      foreach {
        case Row(id: Long, text: String, prob: Vector, prediction: Double) =>
          println(s"($id, $text) --> prob=$prob, prediction=$prediction")
      }
  }

  val conf = new SparkConf().setAppName("SparkSql").setMaster("local[2]")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)
  // 在Scala中使用反射方式，进行RDD到DataFrame的转换，需要手动导入一个隐式转换
  import sqlContext.implicits._

  val spark = SparkSession.builder().getOrCreate()
}