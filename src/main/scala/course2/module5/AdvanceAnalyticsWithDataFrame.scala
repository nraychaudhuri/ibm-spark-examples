package course2.module5

import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object AdvanceAnalyticsWithDataFrame {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("Spark DataFrames")
    // Change to a more reasonable default number of partitions for our data
    // (from 200)
    conf.set("spark.sql.shuffle.partitions", "4")
    conf.set("spark.app.id", "Console")   // To silence Metrics warning.
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val sentenceData = sqlContext.createDataFrame(Seq(
      (0, "Hi I heard about Spark"),
      (0, "I wish Java could use case classes"),
      (1, "Logistic regression models are neat")
    )).toDF("label", "sentence")
    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
    val wordsData = tokenizer.transform(sentenceData)
    val hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(20)
    val featurizedData = hashingTF.transform(wordsData)
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)
    val rescaledData = idfModel.transform(featurizedData)
    rescaledData.select("features", "label").take(3).foreach(println)
  }
}
