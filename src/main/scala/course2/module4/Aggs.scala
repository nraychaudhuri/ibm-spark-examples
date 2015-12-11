package course2.module4

import data.Flight
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import util._

/**
 * Aggregations with Spark DataFrames.
 */
object Aggs {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("Spark DataFrame Aggregations")
    conf.set("spark.sql.shuffle.partitions", "4")
    conf.set("spark.app.id", "Aggs")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    import sqlContext.sql
    import org.apache.spark.sql.functions._  // for min, max, etc.

    try {

      val flightsPath =
        "data/airline-flights/alaska-airlines/2008.csv"
      val flightsRDD = for {
        line <- sc.textFile(flightsPath)
        flight <- Flight.parse(line)
      } yield flight
      val flights  = sqlContext.createDataFrame(flightsRDD)
      flights.cache
      println("Flights:")
      flights.printSchema
      flights.show()

      flights.registerTempTable("flights")

      flights.agg(
        min($"times.actualElapsedTime"),
        max($"times.actualElapsedTime"),
        avg($"times.actualElapsedTime"),
        sum($"times.actualElapsedTime")).show()

      // Let's also see the corresponding SQL query:

      sql("""
        SELECT
          MIN(times.actualElapsedTime) AS min,
          MAX(times.actualElapsedTime) AS max,
          AVG(times.actualElapsedTime) AS avg,
          SUM(times.actualElapsedTime) AS sum
        FROM flights
        """).show()


      flights.agg(count($"*")).show       // i.e. COUNT(*)
      flights.agg(count($"tailNum")).show // same; no tailNum = NULL
      flights.agg(countDistinct($"tailNum")).show()
      flights.agg(approxCountDistinct($"tailNum")).show()

      sql("SELECT COUNT(*) AS count FROM flights").show()
      sql("SELECT COUNT(tailNum) AS count FROM flights").show()
      sql("SELECT COUNT(DISTINCT tailNum) AS countd FROM flights").show()
      // No corresponding approximate count in Spark's SQL dialect

    } finally {
      sc.stop()
    }
  }
}