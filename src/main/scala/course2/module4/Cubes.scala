package course2.module4

import data.Flight
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import util._

/**
 * Analytics cubes with Spark DataFrames.
 */
object Cubes {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("Spark DataFrame Cubes")
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

      // Compute the average of all numeric fields,
      // cubed by (per) origin and dest.
      flights.cube("origin", "dest").avg().show()

      flights.cube("origin", "dest").avg().select(
        "origin", "dest", "avg(distance)").filter(
        $"origin" === "LAX").show()

      // No equivalent SQL query; we have to name
      // the columns explicitly.
      sql("""
        SELECT origin, dest,
        AVG(flightNum),
        AVG(distance),
        AVG(canceled),
        AVG(diverted),
        AVG(carrierDelay),
        AVG(weatherDelay),
        AVG(nasDelay),
        AVG(securityDelay),
        AVG(lateAircraftDelay)
        FROM flights
        GROUP BY origin, dest
        """).show()

      // Compute the average elapsed time and distance,
      // cubed by origin and dest.
      // Compute aggregates by specifying a map from
      // column name to aggregate methods.

      flights.cube($"origin", $"dest").agg(Map(
        "*" -> "count",
        "times.actualElapsedTime" -> "avg",
        "distance" -> "avg"
      )).orderBy($"avg(distance)".desc).show()

      sql("""
        SELECT origin, dest,
          COUNT(*) AS count,
          AVG(times.actualElapsedTime) AS avg_time,
          AVG(distance) AS avg_dist
        FROM flights
        GROUP BY origin, dest
        ORDER BY avg_dist DESC
        """).show()

      // The previous method isn't very flexible. We can't
      // do math expressions and we can't reference a column
      // more than once. This alternative gives us the
      // flexibility we need.

      val dist_time = flights.cube($"origin", $"dest").agg(
        avg("distance").as("avg_dist"),
        min("times.actualElapsedTime").as("min_time"),
        max("times.actualElapsedTime").as("max_time"),
        avg("times.actualElapsedTime").as("avg_time"),
        (avg("distance") / avg("times.actualElapsedTime")).as("t_d")
      ).orderBy($"avg_dist".desc).cache

      println("For longest flights:")
      dist_time.show()
      println("For shortest flights:")
      dist_time.orderBy($"avg_dist".asc).show()
      dist_time.unpersist

      val dist_time_sql = sql("""
        SELECT origin, dest,
          AVG(distance) AS avg_dist,
          MIN(times.actualElapsedTime) AS min_time,
          MAX(times.actualElapsedTime) AS max_time,
          AVG(times.actualElapsedTime) AS avg_time,
          (AVG(distance) / AVG(times.actualElapsedTime)) AS t_d
        FROM flights
        GROUP BY origin, dest
        ORDER BY avg_dist DESC
        """).cache

      dist_time_sql.show

      // Flight delays:
      val delays = flights.cube($"origin", $"dest").avg(
        "canceled",
        "weatherDelay"
      ).toDF("origin", "dest", "canceled", "weatherDelay").cache
      delays.orderBy($"canceled").show
      delays.orderBy($"canceled".desc).show
      delays.orderBy($"weatherDelay").show
      delays.orderBy($"weatherDelay".desc).show
      delays.unpersist

      sql("""
        SELECT origin, dest,
          AVG(canceled) AS canceled,
          AVG(weatherDelay) AS weatherDelay
        FROM flights
        GROUP BY origin, dest
        ORDER BY canceled DESC
        """).show

    } finally {
      sc.stop()
    }
  }
}