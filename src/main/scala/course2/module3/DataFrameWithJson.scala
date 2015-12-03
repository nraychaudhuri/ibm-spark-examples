package course2.module3

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
    conf.set("spark.app.id", "DataFrameWithJson")   // To silence Metrics warning.
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    try {

      //each line should have a complete json record
      val json: DataFrame =
        sqlContext.read.json("data/airline-flights/carriers.json")

      //spark infers the schema as it reads the json document. Since there is a invalid
      //json record the schema will have an additional column called _corrupt_record
      //for invalid json record.
      // It doesn't stop the processing when it finds an invalid records which is great for
      //large jobs as you don't want to stop for each invalid data record
      json.printSchema()
      Printer(out, "Flights between airports, sorted by airports", json)


      //Printing out the records that failed to parse
      json.where(json("_corrupt_record").isNotNull).collect().foreach(println)
    } finally {
      sc.stop()
    }

  }
}
