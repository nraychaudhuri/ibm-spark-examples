package course2.module4

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.scheduler.{
  StreamingListener, StreamingListenerReceiverError, StreamingListenerReceiverStopped}
import org.apache.spark.sql.hive.HiveContext
import data.Flight
import util.Files
import scala.util.control.NonFatal

/**
 * Sketch of an ETL pipeline into Hive using Spark Streaming.
 * Data is written on a socket running in one thread while a Spark Streaming
 * app runs in another set of threads to process the incoming data.
 */
object HiveETL {

  val defaultPort = 9000
  val interval = Seconds(5)
  val pause = 10  // milliseconds
  val server = "127.0.0.1"
  val hiveETLDir = "output/hive-etl"
  val checkpointDir = "output/checkpoint_dir"
  val runtime = 10 * 1000   // run for N*1000 milliseconds
  val numRecordsToWritePerBlock = 10000

  def main(args: Array[String]): Unit = {
    val port = if (args.size > 0) args(0).toInt else defaultPort
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("ETL with Spark Streaming and Hive")
    conf.set("spark.sql.shuffle.partitions", "4")
    conf.set("spark.app.id", "HiveETL")
    val sc = new SparkContext(conf)

    // Clean up the checkpoint directory, the in-memory metastore,
    // the derby log and the output of the last run, if any.
    // DON'T do any of this in production, under normal circumstances!
    Files.rmrf("derby.log")
    Files.rmrf("metastore_db")
    Files.rmrf(checkpointDir)
    Files.rmrf(hiveETLDir)

    def createContext(): StreamingContext = {
      val ssc = new StreamingContext(sc, interval)
      ssc.checkpoint(checkpointDir)

      val dstream = readSocket(ssc, server, port)
      processDStream(ssc, dstream)
    }

    // Declared as a var so we can see it in the following try/catch/finally blocks.
    var ssc: StreamingContext = null
    var dataThread: Thread = null

    try {

      banner("Creating source socket:")
      dataThread = startSocketDataThread(port)

      // The preferred way to construct a StreamingContext, because if the
      // program is restarted, e.g., due to a crash, it will pick up where it
      // left off. Otherwise, it starts a new job.
      ssc = StreamingContext.getOrCreate(checkpointDir, createContext _)
      ssc.addStreamingListener(new EndOfStreamListener(ssc, dataThread))
      ssc.start()
      ssc.awaitTerminationOrTimeout(runtime)

    } finally {
      shutdown(ssc, dataThread)
    }
  }

  def readSocket(ssc: StreamingContext, server: String, port: Int): DStream[String] =
    try {
      banner(s"Connecting to $server:$port...")
      ssc.socketTextStream(server, port)
    } catch {
      case th: Throwable =>
        ssc.stop()
        throw new RuntimeException(
          s"Failed to initialize server:port socket with $server:$port:",
          th)
    }

  import java.net.{Socket, ServerSocket}
  import java.io.{File, PrintWriter}

  // A Runnable to send the Airline flights data to a socket.
  // It sleeps temporarily after each numRecordsToWritePerBlock
  // block.
  def makeRunnable(port: Int) = new Runnable {
    def run() = {
      val listener = new ServerSocket(port);
      var socket: Socket = null
      try {
        val socket = listener.accept()
        val out = new PrintWriter(socket.getOutputStream(), true)
        val inputPath = "data/airline-flights/alaska-airlines/2008.csv"
        // A hack to write N lines at a time, then sleep...
        var lineCount = 0
        var passes = 0
        scala.io.Source.fromFile(inputPath).getLines().foreach {line =>
          out.println(line)
          if (lineCount % numRecordsToWritePerBlock == 0) Thread.sleep(pause)
          lineCount += 1
        }
      } finally {
        listener.close();
        if (socket != null) socket.close();
      }
    }
  }

  def startSocketDataThread(port: Int): Thread = {
    val dataThread = new Thread(makeRunnable(port))
    dataThread.start()
    dataThread
  }

  // Does the real work: creates the Hive table, ingests the socket data,
  // parses into flights, splits the flights into year-month-day and writes
  // to new Hive table partitions.
  def processDStream(ssc: StreamingContext, dstream: DStream[String]): StreamingContext = {
    val hiveContext = new HiveContext(ssc.sparkContext)
    import hiveContext.implicits._
    import hiveContext.sql
    import org.apache.spark.sql.functions._  // for min, max, etc.

    // We'll use the flights data. We're going to do a little more work
    // than necessary; Hive could partition creation for us, for example,
    // if we use INSERT INTO, with values for new partition columns, etc.

    // Before processing the stream, create the table.
    val hiveETLFile = new java.io.File(hiveETLDir)
    // Hive DDL statements require absolute paths:
    val hiveETLPath = hiveETLFile.getCanonicalPath
    banner(s"""
      Create an 'external', partitioned Hive table for a subset of the flight data:
      Location: $hiveETLPath
      """)

    sql(s"""
      CREATE EXTERNAL TABLE IF NOT EXISTS flights2 (
        depTime        INT,
        arrTime        INT,
        uniqueCarrier  STRING,
        flightNum      INT,
        origin         STRING,
        dest           STRING)
      PARTITIONED BY (
        depYear        STRING,
        depMonth       STRING,
        depDay         STRING)
      ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
      LOCATION '$hiveETLPath'
      """).show
    println("Tables:")
    sql("SHOW TABLES").show

    dstream.foreachRDD { (rdd, timestamp) =>
      try {
        // Ignore bad lines that fail to parse (for simplicity).
        val flights =
          rdd.flatMap(line => Flight.parse(line)).toDF().cache

        // Find the unique year-month-day combinations in the data.
        // It appears to be safer to "collect" to a collection in
        // the driver and then go through it serially to create
        // corresponding table partitions.
        val uniqueYMDs = flights
          .select("date.year", "date.month", "date.dayOfMonth")
          .distinct.collect

        // For each year-month-day, create the partition, filter
        // the data for that date, form the new records, and write
        // it to the partition directory. We write '|'-separated
        // strings. Consider replacing with Parquet in production.
        uniqueYMDs.foreach { row =>
          val year     = row.getInt(0)
          val month    = row.getInt(1)
          val day      = row.getInt(2)
          val yearStr  = "%04d".format(year)
          val monthStr = "%02d".format(month)
          val dayStr   = "%02d".format(day)
          val partitionPath ="%s/%s-%s-%s".format(
            hiveETLPath, yearStr, monthStr, dayStr)
          println(s"(time: $timestamp) Creating partition: $partitionPath")
          sql(s"""
            ALTER TABLE flights2 ADD IF NOT EXISTS PARTITION (
              depYear='$yearStr', depMonth='$monthStr', depDay='$dayStr')
            LOCATION '$partitionPath'
            """)

          flights
            .where(
              $"date.year" === year and
              $"date.month" === month and
              $"date.dayOfMonth" === day)
            // DON'T write the partition columns.
            .select($"times.depTime", $"times.arrTime", $"uniqueCarrier",
                $"flightNum", $"origin", $"dest")
            .map(row => row.mkString("|"))
            .saveAsTextFile(partitionPath)
        }

        val showp = sql("SHOW PARTITIONS flights2")
        val showpCount = showp.count
        println(s"(time: $timestamp) Partitions (${showpCount}):")
        showp.foreach(p => println("  "+p))
      } catch {
        case NonFatal(ex) =>
          banner("Exception: "+ex)
          sys.exit(1)
      }
    }
    ssc
  }

  protected def banner(message: String): Unit = {
    println("\n*********************\n")
    message.split("\n").foreach(line => println("    "+line))
    println("\n*********************\n")
  }

  protected def shutdown(ssc: StreamingContext, dataThread: Thread) = {
    banner("Shutting down...")
    if (dataThread != null) dataThread.interrupt()
    else ("The dataThread is null!")
    if (ssc != null) ssc.stop(stopSparkContext = true, stopGracefully = true)
    else ("The StreamingContext is null!")
  }

  class EndOfStreamListener(ssc: StreamingContext, dataThread: Thread) extends StreamingListener {
    override def onReceiverError(error: StreamingListenerReceiverError):Unit = {
      banner(s"Receiver Error: $error. Stopping...")
      shutdown(ssc, dataThread)
    }
    override def onReceiverStopped(stopped: StreamingListenerReceiverStopped):Unit = {
      banner(s"Receiver Stopped: $stopped. Stopping...")
      shutdown(ssc, dataThread)
    }
  }
}
