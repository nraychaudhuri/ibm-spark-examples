package course2.module2

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object DataFrameWithCsv {
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
      val df = sqlContext.read
        //Specifies the input data source format. The readers and writers
        //of this format is provided by the databricks-csv library. This also shows
        //how to add support for custom data sources.
        .format("com.databricks.spark.csv")
        .option("header", "true") // Use first line of all files as header
        .option("inferSchema", "true") // Automatically infer data types
        .load("data/airline-flights/carriers.csv")

      df.printSchema()
      df.show()

    } finally {
      sc.stop()
    }

  }

}
