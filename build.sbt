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
  "org.apache.spark" %% "spark-mllib"        % sparkVersion withSources(),
  "com.databricks"   %% "spark-csv"          % "1.3.0"      withSources()
)

unmanagedResourceDirectories in Compile += baseDirectory.value / "conf"
unmanagedResourceDirectories in Test += baseDirectory.value / "conf"

initialCommands += """
  import org.apache.spark.SparkContext
  import org.apache.spark.SparkContext._
  import org.apache.spark.sql.SQLContext
  val conf = new SparkConf().
    setMaster("local[*]").
    setAppName("Console").
    set("spark.app.id", "Console")   // To silence Metrics warning.
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
addCommandAlias("ex3",          "run-main course2.module3.SparkDataFrames")
addCommandAlias("ex3-csv",      "run-main course2.module3.DataFrameWithCsv")
addCommandAlias("ex3-json",     "run-main course2.module3.DataFrameWithJson")
addCommandAlias("ex3-parquet",  "run-main course2.module3.DataFrameWithParquet")
addCommandAlias("ex5",          "run-main course2.module5.AdvanceAnalyticsWithDataFrame")
addCommandAlias("ex5-supervised", "run-main course2.module5.SupervisedLearningExample")
addCommandAlias("ex5-unsupervised", "run-main course2.module5.UnsupervisedLearningExample")
addCommandAlias("ex5-graphx", "run-main course2.module5.GraphingFlights")
