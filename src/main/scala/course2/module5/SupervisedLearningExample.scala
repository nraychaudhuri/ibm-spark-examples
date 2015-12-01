package course2.module5


import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

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

    Logger.getRootLogger.setLevel(Level.WARN)

    val ratingsData = sc.textFile(ratingsFile).map { line =>
      val fields = line.split("::")
      Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble)
    }.cache()

    val testData = sc.textFile(testFile).map { line =>
      val fields = line.split(",")
      // format: (movieId, movieName)
      (fields(0).toInt, fields(1).toInt)
    }

    val movies = sc.textFile(moviesFile).map { line =>
      val fields = line.split("::")
      // format: (userId, movieId)
      (fields(0).toInt, fields(1))
    }.collect().toMap


    val numRatings = ratingsData.count()
    val numUsers = ratingsData.map(_.user).distinct().count()
    val numMovies = ratingsData.map(_.product).distinct().count()

    println(s"Got $numRatings ratings from $numUsers users on $numMovies movies.")

    val numTraining = ratingsData.count()
    val numTest = testData.count()
    println(s"Training: $numTraining, test: $numTest.")

    //This is much more simplified version, in real world we try different rank, iterations and lambda
    //values to find best model.
    val model: MatrixFactorizationModel = ALS.train(ratingsData, rank, numIterations)

    val recommendations: RDD[Rating] = model.predict(testData)
    recommendations.foreach(r => println(s"Recommendation for User ${r.user} ${movies(r.product)}"))

    //recommend products for User 0
    println("Recommend movies for User 0 based on ratings")
    model.recommendProducts(0, 5).foreach(r => println(movies(r.product)))

    sc.stop()
  }
}
