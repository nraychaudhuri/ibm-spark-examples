package course2.module1

import org.apache.spark.SparkContext


object WordCountExample {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local[*]", "Word Count")
    try {
      val input = sc.textFile("data/all-shakespeare.txt").map(_.toLowerCase)
      val wc = input
        .flatMap(text => text.split("""\W+"""))
        .map(word => (word, 1))  // RDD[(String,Int)]
        .reduceByKey((n1, n2) => n1 + n2)   // or .reduceByKey(_ + _)

      val outpath = s"output/wc-${System.currentTimeMillis()}"
      println(s"Writing output to: $outpath")
      wc.saveAsTextFile(outpath)
      printMsg("Enter any key to finish the job...")
      Console.in.read()
    } finally {
      sc.stop()
    }
  }


  def printMsg(m: String) = {
    println("")
    println(m)
    println("")
  }
}
