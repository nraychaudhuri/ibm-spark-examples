package course2.module3

import data.{Airport, Flight}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import util._

/**
 * Example of Spark DataFrames, a Python Pandas-like API built on top of
 * SparkSQL with better performance than RDDs, due to the use of Catalyst
 * for "query" optimization.
 */
object SparkDataFrames {

  var out = Console.out   // Overload for tests
  var quiet = false

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("Spark DataFrames")
    // Change to a more reasonable default number of partitions for our data
    // (from 200)
    conf.set("spark.sql.shuffle.partitions", "4")
    conf.set("spark.app.id", "SparkDataFrames")   // To silence Metrics warning.
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._  // Needed for column idioms like $"foo".desc.


    try {

      val flightsPath = "data/airline-flights/alaska-airlines/2008.csv"
      val airportsPath = "data/airline-flights/airports.csv"
      // Don't "pre-guess" keys; just use the types as Schemas.
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
      // Cache just the flights and airports.
      flights.cache
      airports.cache

      // SELECT COUNT(*) FROM flights f WHERE f.canceled > 0;
      val canceled_flights = flights.filter(flights("canceled") > 0)
      Printer(out, "canceled flights", canceled_flights)
      if (!quiet) {
        out.println("\ncanceled_flights.explain(extended = false):")
        canceled_flights.explain(extended = false)
        out.println("\ncanceled_flights.explain(extended = true):")
        canceled_flights.explain(extended = true)
      }
      canceled_flights.cache

      // Note how we can reference the columns several ways:
      if (!quiet) {
        flights.orderBy(flights("origin")).show
        flights.orderBy("origin").show
        flights.orderBy($"origin").show
        flights.orderBy($"origin".desc).show
      }
      // The last one $"count".desc is the only (?) way to specify descending order.
      // The $"..." is not a Scala built-in feature, but Scala allows you to
      // implement "interpolated string" handlers with your own prefix ($ in
      // this case).

      // SELECT cf.date.month AS month, COUNT(*)
      //   FROM canceled_flights cf
      //   GROUP BY cf.date.month
      //   ORDER BY month;
      val canceled_flights_by_month = canceled_flights.
        groupBy("date.month").count()
      Printer(out, "canceled flights by month", canceled_flights_by_month)
      if (!quiet) {
        out.println("\ncanceled_flights_by_month.explain(true):")
        canceled_flights_by_month.explain(true)
      }
      canceled_flights.unpersist

      // Watch what happens with the next calculation.
      // Before running the next query, change the shuffle.partitions property to 50:
      sqlContext.setConf("spark.sql.shuffle.partitions", "50")

      // We used "50" instead of "4". Run the query. How much time does it take?
      //
      // SELECT origin, dest, COUNT(*) AS cnt
      //   FROM flights
      //   GROUP BY origin, dest
      //   ORDER BY cnt DESC, origin, dest;
      val flights_between_airports50 = flights.select($"origin", $"dest").
        groupBy($"origin", $"dest").count().
        orderBy($"count".desc, $"origin", $"dest")
      Printer(out, "Flights between airports, sorted by airports", flights_between_airports50)

      // Now change it back, run the query and compare the time. Does the output change?
      sqlContext.setConf("spark.sql.shuffle.partitions", "4")
      val flights_between_airports = flights.select($"origin", $"dest").
        groupBy($"origin", $"dest").count().
        orderBy($"count".desc, $"origin", $"dest")
      Printer(out, "Flights between airports, sorted by airports", flights_between_airports)
      if (!quiet) {
        println("\nflights_between_airports.explain(true):")
        flights_between_airports.explain(true)
      }

      flights_between_airports.cache

      // Now note it's sometimes useful to coalesce to a smaller number of partitions
      // after calling an operation like `groupBy`, where the number of resulting
      // records may drop dramatically (but they records become correspondingly bigger!).
      // Specifically, `groupBy` returns a `GroupedData` object, on which we call
      // `count`, which returns a new `DataFrame`. That's what we coalesce on so that
      // `orderBy` can potentially be much faster. HOWEVER, because we set the default
      // number of partitions to 4 with the property above, in this particular case,
      // this doesn't make much difference.
      val flights_between_airports2 = flights.
        groupBy($"origin", $"dest").count().coalesce(2).
        sort($"count".desc, $"origin", $"dest")
      Printer(out, "Flights between airports, sorted by airports", flights_between_airports2)
      if (!quiet) {
        println("\nflights_between_airports2.explain(true):")
        flights_between_airports2.explain(true)
      }

      // SELECT origin, dest, COUNT(*)
      //   FROM flights_between_airports
      //   ORDER BY count DESC;
      val frequent_flights_between_airports =
        flights_between_airports.orderBy($"count".desc)
      // Show all of them (~170)
      Printer(out, "Flights between airports, sorted by counts descending", frequent_flights_between_airports, 200)
      if (!quiet) {
        out.println("\nfrequent_flights_between_airports.explain(true):")
        frequent_flights_between_airports.explain(true)
      }



    } finally {
      sc.stop()
    }
  }
}
