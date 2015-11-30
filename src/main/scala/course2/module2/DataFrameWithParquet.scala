package course2.module2

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}
import util.Printer

object DataFrameWithParquet {

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

      //writing 100 json records to parquet format and reading it back again
      val carriers = sqlContext.read.json("data/airline-flights/carriers.json")
      val outPath = s"output/carriers-${System.currentTimeMillis()}.parquet"
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
