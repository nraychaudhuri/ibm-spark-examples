package course2.module4

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import data.Flight
import util.Files
import scala.util.control.NonFatal

/**
 * Run queries over the data created by HiveETL, as a check.
 */
object HiveETLCheck {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("ETL Spark Streaming and Hive - Check")
    conf.set("spark.sql.shuffle.partitions", "4")
    conf.set("spark.app.id", "HiveETLCheck")
    val sc = new SparkContext(conf)
    val hiveContext = new HiveContext(sc)
    import hiveContext.implicits._
    import hiveContext.sql
    import org.apache.spark.sql.functions._  // for min, max, etc.

    try {

      sql("SHOW TABLES").show
      sql("DESCRIBE flights2").show(100)
      sql("DESCRIBE FORMATTED flights2").show(100, truncate = false)

      // Tip: IF the following crashes with an error that you can't
      // access "derby", then delete the metastore_db/*.lck files.
      sql("SELECT COUNT(*) FROM flights2").show

      println("How many flights per year, month, and day?")
      sql("""
        SELECT depyear, depmonth, depday, COUNT(*) FROM flights2
        GROUP BY depyear, depmonth, depday
        """).show(400, truncate = false)

    } finally {
      sc.stop()
    }
  }
}
