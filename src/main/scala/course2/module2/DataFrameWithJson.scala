package course2.module2

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, DataFrame, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}
import util.Printer

object DataFrameWithJson {

  var out = Console.out
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("Spark DataFrames")
    // Change to a more reasonable default number of partitions for our data
    // (from 200)
    conf.set("spark.sql.shuffle.partitions", "4")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    try {
      val json: DataFrame =
        sqlContext.read.json("data/airline-flights/carriers.json")

      json.printSchema()
      Printer(out, "Flights between airports, sorted by airports", json)
      println(">>>>>>>>>>>>>>>>>>>>>>")
      json.where(json("_corrupt_record").isNotNull).collect().foreach(println)
      println(">>>>>>>>>>>>>>>>>>>>>>")
    } finally {
      sc.stop()
    }

  }
}
