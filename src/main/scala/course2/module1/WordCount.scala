package course2.module1

import org.apache.spark.SparkContext
import util.Files

/**
 * Count the words in a corpus of documents.
 * This version uses the familiar GROUP BY, but we'll see a more efficient
 * version next.
 */
object WordCount {

  def main(args: Array[String]): Unit = {
    val inpath  = "data/all-shakespeare.txt"
    val outpath = "output/word_count1"
    Files.rmrf(outpath)  // delete old output (DON'T DO THIS IN PRODUCTION!)

    val sc = new SparkContext("local[*]", "Word Count")
    try {
      val input = sc.textFile(inpath)
      val wc = input
        .map(_.toLowerCase)
        .flatMap(text => text.split("""\W+"""))
        .groupBy(word => word)  // Like SQL GROUP BY: RDD[(String,Iterator[String])]
        .mapValues(group => group.size)  // RDD[(String,Int)]

      println(s"Writing output to: $outpath")
      wc.saveAsTextFile(outpath)
      printMsg("Enter any key to finish the job...")
      Console.in.read()
    } finally {
      sc.stop()
    }
  }


  private def printMsg(m: String) = {
    println("")
    println(m)
    println("")
  }
}
