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