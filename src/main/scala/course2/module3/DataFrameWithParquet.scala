package course2.module3

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import util.Files

object DataFrameWithParquet {

  var out = Console.out
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("Spark DataFrames")
    // Change to a more reasonable default number of partitions for our data
    // (from 200)
    conf.set("spark.sql.shuffle.partitions", "4")
    conf.set("spark.app.id", "DataFrameWithParquet")   // To silence Metrics warning.
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    try {

      //writing 100 json records to parquet format and reading it back again
      val carriers = sqlContext.read.json("data/airline-flights/carriers.json")
      val outPath = "output/carriers.parquet"
      Files.rmrf(outPath)
      carriers.limit(100).write.parquet(outPath)

      out.println(s"Reading in the Parquet file from $outPath:")
      val carriers2 = sqlContext.read.parquet(outPath)
      carriers2.printSchema
      carriers2.show

    } finally {
      sc.stop()
    }

  }

}
