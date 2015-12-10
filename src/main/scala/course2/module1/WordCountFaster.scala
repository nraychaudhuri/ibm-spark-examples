package course2.module1

import org.apache.spark.SparkContext
import util.Files

/**
 * Count the words in a corpus of documents.
 * Uses the more efficient `reduceByKey` instead of `groupBy`.
 */
object WordCountFaster {

  def main(args: Array[String]): Unit = {
    val inpath  = "data/all-shakespeare.txt"
    val outpath = "output/word_count2"
    Files.rmrf(outpath)  // delete old output (DON'T DO THIS IN PRODUCTION!)

    val sc = new SparkContext("local[*]", "Word Count")
    try {
      val wc = sc.textFile(inpath)
        .map(_.toLowerCase)
        .flatMap(text => text.split("""\W+"""))
        .map(word => (word, 1))  // RDD[(String,Int)]
        .reduceByKey((n1, n2) => n1 + n2)   // or .reduceByKey(_ + _)

      println(s"Writing output to: $outpath")
      wc.saveAsTextFile(outpath)
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
