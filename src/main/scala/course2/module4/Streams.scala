package course2.module4

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.Seconds

/**
 * Demonstration of Spark Streaming. Random numbers written on a socket
 * running in one thread while a Spark Streaming app runs in another
 * set of threads to process the incoming data.
 */
object Streams {

  val defaultPort = 9000
  val interval = Seconds(2)
  val pause = 10  // milliseconds
  val server = "127.0.0.1"
  val checkpointDir = "output/checkpoint_dir"
  val runtime = 30 * 1000   // run for N*1000 milliseconds
  val numIterations = 100000

  def main(args: Array[String]): Unit = {
    val port = if (args.size > 0) args(0).toInt else defaultPort
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("Spark DataFrames")
    conf.set("spark.sql.shuffle.partitions", "4")
    conf.set("spark.app.id", "Aggs")
    val sc = new SparkContext(conf)

    def createContext(): StreamingContext = {
      val ssc = new StreamingContext(sc, interval)
      ssc.checkpoint(checkpointDir)

      val dstream = readSocket(ssc, server, port)
      // While we write the numbers one per line, we don't assume
      // this here; we assume multiple, space-separated numbers
      // might be on each line.
      val numbers = for {
        line <- dstream
        number <- line.trim.split("\\s+")
      } yield number.toInt
      numbers.foreachRDD { rdd =>
        rdd.countByValue.foreach(println)
      }

      ssc
    }

    // Declared as a var so we can see it in the following try/catch/finally blocks.
    var ssc: StreamingContext = null
    var dataThread: Thread = null

    try {

      println("Creating source socket:")
      dataThread = startSocketDataThread(port)

      // The preferred way to construct a StreamingContext, because if the
      // program is restarted, e.g., due to a crash, it will pick up where it
      // left off. Otherwise, it starts a new job.
      ssc = StreamingContext.getOrCreate(checkpointDir, createContext _)
      ssc.start()
      ssc.awaitTerminationOrTimeout(runtime)

    } finally {
      if (dataThread != null) dataThread.interrupt()
      if (ssc != null)
        ssc.stop(stopSparkContext = true, stopGracefully = true)
    }
  }

  def readSocket(ssc: StreamingContext, server: String, port: Int): DStream[String] =
    try {
      Console.err.println(s"Connecting to $server:$port...")
      ssc.socketTextStream(server, port)
    } catch {
      case th: Throwable =>
        ssc.stop()
        throw new RuntimeException(
          s"Failed to initialize server:port socket with $server:$port:",
          th)
    }

  import java.net.{Socket, ServerSocket}
  import java.io.PrintWriter

  def makeRunnable(port: Int) = new Runnable {
    def run() = {
      val listener = new ServerSocket(port);
      var socket: Socket = null
      try {
        val socket = listener.accept()
        val out = new PrintWriter(socket.getOutputStream(), true)
        (1 to numIterations).foreach { i =>
          val number = (100 * math.random).toInt
          out.println(number)
          Thread.sleep(pause)
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

}
