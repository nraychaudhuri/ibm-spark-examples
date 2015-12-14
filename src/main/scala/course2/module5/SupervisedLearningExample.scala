package course2.module5


import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.recommendation.{ALSModel, ALS}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

case class Rating(userId: Int, movieId: Int, rating: Double)

object Rating {
  //format is: userId, movieId, rating, timestamp
  def parseRating(str: String): Rating = {
    val fields = str.split("::")
    Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble) //we don't care about the timestamp
  }
}

case class Movie(movieId: Int, title: String, genres: Seq[String])

object Movie {
  //format is: movieId, title, genre1|genre2
  def parseMovie(str: String): Movie = {
    val fields = str.split("::")
    assert(fields.size == 3)
    Movie(fields(0).toInt, fields(1), fields(2).split("|"))
  }
}


//we will take an Collaborative filtering example to rate movies.
//The data is from (http://grouplens.org/datasets/movielens/). MovieLens is a
//non-commercial movie recommendation website
//The example is inspired from the MovieLensALS.scala example from spark distribution.
object SupervisedLearningExample {

  val numIterations = 10
  val rank = 10 //number of features to consider when training the model
  //this file is in UserID::MovieID::Rating::Timestamp format
  val ratingsFile = "data/als/sample_movielens_ratings.txt"
  val moviesFile = "data/als/sample_movielens_movies.txt"
  val testFile = "data/als/test.data"

  def main(args: Array[String]): Unit = run()

  def run(): Unit = {
    val conf = new SparkConf()
      .setAppName("SupervisedLearning")
      .setMaster("local[*]")
      .set("spark.app.id", "ALS")   // To silence Metrics warning.

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    Logger.getRootLogger.setLevel(Level.WARN)


    //this will be our training dataset
    val ratingsData: RDD[Rating] = sc.textFile(ratingsFile).map(Rating.parseRating).cache()

    //this will be out test dataset to verify the model
    val testData: RDD[Rating] = sc.textFile(testFile).map(Rating.parseRating).cache()


    val numRatings = ratingsData.count()
    val numUsers = ratingsData.map(_.userId).distinct().count()
    val numMovies = ratingsData.map(_.movieId).distinct().count()

    println(s"Got $numRatings ratings from $numUsers users on $numMovies movies.")


    //This is much more simplified version, in real world we try different rank,
    // iterations and other parameters to find best model.
    //typically ALS model looks for two properties, usercol for user info and itemcol for items that we are recommending
    val als = new ALS()
                 .setUserCol("userId")
                 .setItemCol("movieId")
                 .setRank(rank)
                 .setMaxIter(numIterations)

    //training the model
    val model: ALSModel = als.fit(ratingsData.toDF) //converting rdd to dataframe for spark.ml

    //now trying the model on our testdata
    val predictions: DataFrame = model.transform(testData.toDF).cache

    //metadata about the movies
    val movies = sc.textFile(moviesFile).map(Movie.parseMovie).toDF()

    //trying to find out if our model has any falsePositives
    val falsePositives = predictions.join(movies) //joining with movies so that we can print the movie names
      .where((predictions("movieId") === movies("movieId"))
      && ($"rating" <= 1) && ($"prediction" >= 4))
      .select($"userId", predictions("movieId"), $"title", $"rating", $"prediction")
    val numFalsePositives = falsePositives.count()
    println(s"Found $numFalsePositives false positives")
    if (numFalsePositives > 0) {
      println(s"Example false positives:")
      falsePositives.limit(100).collect().foreach(println)
    }

    //show first 20 predictions
    predictions.show()


    //running predictions for user 26 as an example to find out whether user likes some movies
    println(">>>> Find out predictions where user 26 likes movies 10, 15, 20 & 25")
    val df26 = sc
      .makeRDD(Seq(26 -> 10, 26 -> 15, 26 -> 20, 26 -> 25))
      .toDF("userId", "movieId")
    model.transform(df26).show()

    sc.stop()
  }
}
