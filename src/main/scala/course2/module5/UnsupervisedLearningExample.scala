package course2.module5

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}

import org.apache.spark.mllib.linalg.{VectorUDT, Vectors}
import org.apache.spark.ml.clustering.{KMeansModel, KMeans}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{StructField, StructType}

//In this case we are using Kmeans algorithm.
//Clustering is the best-known type of unsupervised learning.
// Clustering algorithms try to find natural groupings in data.
// Data points that are like one another, but unlike others,
// are likely to represent a meaningful grouping, and so clustering
// algorithms try to put such data into the same cluster.
//This example is also taken from Spark distribution and simplified.
object UnsupervisedLearningExample {

  val FEATURES_COL = "features"

  def main(args: Array[String]): Unit = run("data/kmeans_data.txt", 3)

  def run(input: String, k: Int): Unit = {
    val conf = new SparkConf()
      .setAppName("UnsupervisedLearning")
      .setMaster("local[*]")
      .set("spark.app.id", "Kmeans") // To silence Metrics warning.

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    Logger.getRootLogger.setLevel(Level.WARN)

    // Loads data
    val rowRDD = sc.textFile(input)
      .filter(_.nonEmpty)
      .map(_.split(" ").map(_.toDouble))
      //Vectors is the input type KMeans algo expects.
      // it represents a numeric vector of features that algo will use to create cluster.
      .map(Vectors.dense)
      .map(Row(_)) //Each element in DataFrame is represented as Row, think of it as database row
      .cache()
    val schema = StructType(Array(StructField(FEATURES_COL, new VectorUDT, false)))
    val dataset = sqlContext.createDataFrame(rowRDD, schema)

    // Trains a k-means model
    val kmeans = new KMeans()
      .setK(k)
      .setMaxIter(10)
      .setFeaturesCol(FEATURES_COL)
    val model: KMeansModel = kmeans.fit(dataset)

    // Shows the result
    println("Final Centers: ")
    model.clusterCenters.zipWithIndex.foreach(println)

    //Now predict centers for following test data
    //centers tells which groups these test data belongs
    val testData = Seq("0.3 0.3 0.3", "8.0 8.0 8.0", "8.0 0.1 0.1")
      .map(_.split(" ").map(_.toDouble))
      .map(Vectors.dense)
    val test = sc.makeRDD(testData).map(Row(_))
    val testDF = sqlContext.createDataFrame(test, schema)

    model.transform(testDF).show()

    sc.stop()
  }
}
