import sbt.complete.Parsers._

name := "spark-examples"
version := "0.1.0"
scalaVersion := "2.10.5"

//default Spark version
val sparkVersion = "1.5.2"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core"         % sparkVersion withSources(),
  "org.apache.spark" %% "spark-streaming"    % sparkVersion withSources(),
  "org.apache.spark" %% "spark-sql"          % sparkVersion withSources(),
  "org.apache.spark" %% "spark-hive"         % sparkVersion withSources(),
  "org.apache.spark" %% "spark-mllib"        % sparkVersion withSources(),
  "com.databricks"   %% "spark-csv"          % "1.3.0"      withSources()
)

unmanagedResourceDirectories in Compile += baseDirectory.value / "conf"
unmanagedResourceDirectories in Test += baseDirectory.value / "conf"

initialCommands += """
  import org.apache.spark.{SparkConf, SparkContext}
  import org.apache.spark.SparkContext._
  import org.apache.spark.sql.SQLContext
  val conf = new SparkConf().
    setMaster("local[*]").
    setAppName("Console").
    set("spark.app.id", "Console").   // To silence Metrics warning.
    set("spark.sql.shuffle.partitions", "4")  // for smaller data sets.
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)
  import sqlContext.implicits._
  """

cleanupCommands += """
  println("Closing the SparkContext:")
  sc.stop()
  """.stripMargin

addCommandAlias("ex1a",         "run-main course2.module1.WordCount")
addCommandAlias("ex1b",         "run-main course2.module1.WordCountFaster")
addCommandAlias("ex2-crawl",    "run-main course2.module2.Crawl")
addCommandAlias("ex2-ii",       "run-main course2.module2.InvertedIndex")
addCommandAlias("ex3",          "run-main course2.module3.SparkDataFrames")
addCommandAlias("ex3-csv",      "run-main course2.module3.DataFrameWithCsv")
addCommandAlias("ex3-json",     "run-main course2.module3.DataFrameWithJson")
addCommandAlias("ex3-parquet",  "run-main course2.module3.DataFrameWithParquet")
addCommandAlias("ex4-joins",    "run-main course2.module4.Joins")
addCommandAlias("ex4-aggs",     "run-main course2.module4.Aggs")
addCommandAlias("ex4-cubes",    "run-main course2.module4.Cubes")
addCommandAlias("ex4-hive",     "run-main course2.module4.Hive")
addCommandAlias("ex4-streams",  "run-main course2.module4.Streams")
addCommandAlias("ex4-hive-etl", "run-main course2.module4.HiveETL")

// Exercise solutions
addCommandAlias("ex2-ii-sort",        "run-main course2.module2.solns.InvertedIndexSortByWordsAndCounts")
addCommandAlias("ex2-ii-stop-words",  "run-main course2.module2.solns.InvertedIndexSortByWordsAndCountsWithStopWordsFiltering")
