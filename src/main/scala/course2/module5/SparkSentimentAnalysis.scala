package course2.module5

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.classification.{NaiveBayesModel, NaiveBayes}
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}

//Generally, regression refers to predicting a
// numeric quantity like size or income or temperature,
// while classification refers to predicting a label
// or cate‐ gory, like “spam” or “picture of a cat.”
//Naive bayes is a classification algorithm
object SparkSentimentAnalysis {


  def main(args: Array[String]) = {
    val conf = new SparkConf()
      .setAppName("SentimentAnalysis")
      .setMaster("local[*]")
      .set("spark.app.id", "Senti")   // To silence Metrics warning.

    Logger.getRootLogger.setLevel(Level.WARN)

    val sc = new SparkContext(conf)

    try {
      val bayesModel: NaiveBayesModel = createModel(sc)

      //we will use hashing to translate words to numeric features
      val htf = new HashingTF()
      //testing the model with some sample tweets.
      //This model is not perfect(mostly like real world) so some predictions
      //might not be correct
      //0 - negative, 2 = neutral, 4 = positive
      println(">>>>>>>1 " + bayesModel.predict(htf.transform("it rains a lot in london".split(" "))))
      println(">>>>>>>2 " + bayesModel.predict(htf.transform("This product sucks".split(" "))))
      println(">>>>>>>3 " + bayesModel.predict(htf.transform("I am feeling very sad".split(" "))))
      println(">>>>>>>4 " + bayesModel.predict(htf.transform("I love this day".split(" "))))
      println(">>>>>>>5 " + bayesModel.predict(htf.transform("Need a hug ".split(" "))))
      println(">>>>>>>6 " + bayesModel.predict(htf.transform("about to file taxes ".split(" "))))
      println(">>>>>>>7 " + bayesModel.predict(htf.transform("some1 hacked my account on aim  now i have to make a new one".split(" "))))
      println(">>>>>>>8 " + bayesModel.predict(htf.transform("I am down".split(" "))))

    } finally {
      sc.stop()
    }
  }



  def createModel(sc: SparkContext): NaiveBayesModel = {
    val htf = new HashingTF()
    val sqlContext = new SQLContext(sc)
    //    data format
    //    0 - the polarity of the tweet (0 = negative, 2 = neutral, 4 = positive)
    //    1 - the id of the tweet (2087)
    //    2 - the date of the tweet (Sat May 16 23:58:44 UTC 2009)
    //    3 - the query (lyx). If there is no query, then this value is NO_QUERY.
    //    4 - the user that tweeted (robotickilldozr)
    //    5 - the text of the tweet (Lyx is cool)
    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "false")
      .option("inferSchema", "true")
      .load("data/testdata.manual.2009.06.14.csv") //training data taken from help.sentiment140.com
      .toDF("polarity", "id", "date", "query", "user", "tweet")

    df.printSchema()
    df.show()

    val labledRdd: RDD[LabeledPoint] = df.select("polarity", "tweet").rdd.map {
      case Row(polarity: Int, tweet: String) =>
        val words = tweet.split(" ")
        LabeledPoint(polarity, htf.transform(words))
    }

    labledRdd.cache()

    //we are using Multinomial Naive Bayes variation by default
    val bayesModel: NaiveBayesModel = NaiveBayes.train(labledRdd)
    bayesModel
  }
}
