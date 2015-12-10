package course2.module5

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

object SparkSentimentAnalysis {


//  def main(args: Array[String]) = {
//    val conf = new SparkConf()
//      .setAppName("SentimentAnalysis")
//      .setMaster("local[*]")
//      .set("spark.app.id", "Senti")   // To silence Metrics warning.
//
//    val sc = new SparkContext(conf)
//    Logger.getRootLogger.setLevel(Level.WARN)
//
//
//    HashingTF()
////    LabeledPoint()
//    val input: RDD[LabeledPoint] = ???
//    NaiveBayes.train(input)
//
//  }
}
