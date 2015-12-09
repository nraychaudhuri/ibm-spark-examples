package course2.module4

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import util._

/**
 * Use Hive with Spark DataFrames.
 */
object Hive {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("Spark DataFrames")
    conf.set("spark.sql.shuffle.partitions", "4")
    conf.set("spark.app.id", "Aggs")
    val sc = new SparkContext(conf)
    val hiveContext = new HiveContext(sc)
    import hiveContext.implicits._
    import hiveContext.sql
    import org.apache.spark.sql.functions._  // for min, max, etc.

    try {

      // A copy of the airports data, in its own directory and cleaned
      // up a bit to remove double quotes, extra commas in names, and
      // extra whitespace. This allows us to easily split the fields
      // on the ",". For real CSV handling, see the CSV library that's
      // available at the Databricks' "packages" website.
      val dir  = "data/airline-flights/hive/airports"
      val data = new java.io.File(dir)
      // Hive DDL statements require absolute paths:
      val path = data.getCanonicalPath

      println("Create an 'external' Hive table for the airports data:")
      sql(s"""
        CREATE EXTERNAL TABLE IF NOT EXISTS airports (
          iata     STRING,
          airport  STRING,
          city     STRING,
          state    STRING,
          country  STRING,
          lat      DOUBLE,
          long     DOUBLE)
        ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
        LOCATION '$path'
        """).show

      sql("SHOW TABLES").show
      sql("DESCRIBE airports").show(100)
      sql("DESCRIBE EXTENDED airports").show(100)
      sql("DESCRIBE FORMATTED airports").show(100)
      sql("DESCRIBE FORMATTED airports").show(100, truncate = false)
      sql("DESCRIBE FORMATTED airports").foreach(println)

      sql("SELECT COUNT(*) FROM airports").show

      println("How many airports per country?")
      sql("""
        SELECT country, COUNT(*) FROM airports
        GROUP BY country
        """).show(100)

      println("There are four non-US airports:")
      sql("""
        SELECT iata, airport, country
        FROM airports WHERE country <> 'USA'
        """).show(100)

      sql("""
        SELECT state, COUNT(*) AS count FROM airports
        WHERE country = 'USA'
        GROUP BY state
        ORDER BY count DESC
        """).show(100)

      println("Hive queries return DataFrames; let's use that API:")
      val latlong = sql("SELECT state, lat, long FROM airports")
      latlong.cube("state").agg(
        min("lat"),
        max("lat"),
        avg("lat"),
        min("long"),
        max("long"),
        avg("long")
        ).show

      sql("DROP TABLE airports").show

    } finally {
      sc.stop()
    }
  }
}