package course2.module4

import data.{Airport, Flight}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import util._

/**
 * Joins with Spark DataFrames.
 */
object Joins {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("Spark DataFrame Joins")
    conf.set("spark.sql.shuffle.partitions", "4")
    conf.set("spark.app.id", "Joins")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    try {

      val af = "data/airline-flights"
      val flightsPath = s"$af/alaska-airlines/2008.csv"
      val airportsPath = s"$af/airports.csv"
      val flightsRDD = for {
        line <- sc.textFile(flightsPath)
        flight <- Flight.parse(line)
      } yield flight

      val airportsRDD = for {
        line <- sc.textFile(airportsPath)
        airport <- Airport.parse(line)
      } yield airport

      val flights  = sqlContext.createDataFrame(flightsRDD)
      val airports = sqlContext.createDataFrame(airportsRDD)
      flights.cache
      airports.cache

      // Look at their schemas and the first 10 records in each:
      println("Flights schema:")
      flights.printSchema

      println("Flights, first 10 records:")
      flights.show(10)    // Omit the argument: default is 20

      println("Airports schema:")
      airports.printSchema

      println("Airports, first 10 records:")
      airports.show(10)

      // Recall this query from module 3:
      val flights_between_airports = flights.select($"origin", $"dest").
        groupBy($"origin", $"dest").count().
        orderBy($"count".desc, $"origin", $"dest")
      println("Flights between airports, sorted by airports:")
      flights_between_airports.show()

      flights_between_airports.cache

      // Let's also see the corresponding SQL queries:

      import sqlContext.sql   // so we can call this method easily.

      flights.registerTempTable("flights")
      airports.registerTempTable("airports")

      val flights_between_airports_sql = sql("""
        SELECT origin, dest, COUNT(*) AS count FROM flights
        GROUP BY origin, dest
        ORDER BY count DESC, origin, dest
        """)

      println("Flights between airports, SQL query:")
      flights_between_airports_sql.show()

      flights_between_airports_sql.cache
      flights_between_airports_sql.registerTempTable("fbasql")

      // Now, let's join this table to the airports to bring in the full
      // names of the origin and destination airports. I'm going to define
      // some aliases to keep things more concise.
      // Note, we're doing an inner join.

      val fba = flights_between_airports
      val air = airports
      val fba2 = fba.
        join(air, fba("origin") === air("iata")).
          select("origin", "airport", "dest", "count").
          toDF("origin", "origin_airport", "dest", "count").
        join(air, $"dest" === air("iata")).
          select("origin", "origin_airport",
            "dest", "airport", "count").
          toDF("origin", "origin_airport",
            "dest", "dest_airport", "count")

      println("fba2 schema:")
      fba2.printSchema

      println("fba2, first 10 recornds:")
      fba2.show(10)    // Omit the argument: default is 10

      // SQL version:
      val fba2_sql = sql("""
        SELECT f.origin, a1.airport AS origin_airport,
               f.dest,   a2.airport AS dest_airport,   f.count
        FROM fbasql f
        JOIN airports a1 ON f.origin = a1.iata
        JOIN airports a2 ON f.dest   = a2.iata
        """)

      println("fba2_sql schema:")
      fba2_sql.printSchema
      println("fba2_sql, first 10 recornds:")
      fba2_sql.show(10)

      // Return all airports, even those that DON'T have flights
      // in our limited Alaska Airlines sample. (LEFT OUTER JOIN)

      println("A left outer join:")
      air.join(fba, air("iata") === fba("origin"), "left_outer").show()

      sql("""
        SELECT * FROM airports a
        LEFT OUTER JOIN fbasql f ON a.iata = f.origin""").show()

    } finally {
      sc.stop()
    }
  }
}