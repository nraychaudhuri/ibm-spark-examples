package course2.module2

import java.io.{File, FilenameFilter}
import scala.io.Source
import util.Files

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

/**
 * Simulate a web crawl to prep. data for InvertedIndex.
 * Crawl uses `SparkContext.wholeTextFiles` to read the files
 * in a directory hierarchy and return a single RDD with records of the form:
 *    `(file_name, file_contents)`
 * After loading with `SparkContext.wholeTextFiles`, we post process
 * the data in two ways. First, the `file_name` will be an
 * absolute path, which is normally what you would want. However, since the
 * leading path is the same for all files, it doesn't add useful information,
 * so we remove it. Second, the `file_contents` still contains line feeds.
 * We remove those, so that `InvertedIndex` can treat each output line of
 * text as a complete record.
 */
object Crawl {
  def main(args: Array[String]): Unit = {

    val inpath  = "data/enron-spam-ham/*" // Note the *
    val outpath = "output/crawl"
    Files.rmrf(outpath)  // delete old output (DON'T DO THIS IN PRODUCTION!)

    val separator = java.io.File.separator

    val sc = new SparkContext("local[*]", "Crawl")

    try {
      val files_contents = sc.wholeTextFiles(inpath).map {
        case (id, text) =>
          val lastSep = id.lastIndexOf(separator)
          val id2 = if (lastSep < 0) id.trim else id.substring(lastSep+1, id.length).trim
          val text2 = text.trim.replaceAll("""\s*\n\s*""", " ")
          (id2, text2)
      }
      println(s"Writing output to: $outpath")
      files_contents.saveAsTextFile(outpath)
    } finally {
      sc.stop()
    }
  }
}