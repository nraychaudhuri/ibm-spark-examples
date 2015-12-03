package course2.module5

import org.apache.spark.{SparkConf, SparkContext}
import util.Files

/**
 * Simulate a web crawl to prep. data for InvertedIndex.
 * Crawl uses <code>SparkContext.wholeTextFiles</code> to read the files
 * in a directory hierarchy and return a single RDD with records of the form:
 *    <code>(file_name, file_contents)</code>
 * After loading with <code>SparkContext.wholeTextFiles</code>, we post process
 * the data in two ways. First, we the <code>file_name</code> will be an
 * absolute path, which is normally what you would want. However, to make it
 * easier to support running the corresponding unit test <code>CrawlSpec</code>
 * anywhere, we strip all leading path elements. Second, the <code>file_contents</code>
 * still contains linefeeds. We remove those, so that <code>InvertedIndex</code>
 * can treat each line as a complete record.
 */
object Crawl {
  def main(args: Array[String]): Unit = {

    val separator = java.io.File.separator
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("Crawl enron emails")
    conf.set("spark.app.id", "Crawl")   // To silence Metrics warning.

    val input = "data/enron-spam-ham/*"
    val output = "output/crawl"
    Files.rmrf(output)
    val sc = new SparkContext(conf)

    try {
      // See class notes above.
      val files_contents = sc.wholeTextFiles(input).map {
        case (id, text) =>
          val lastSep = id.lastIndexOf(separator)
          val id2 = if (lastSep < 0) id.trim else id.substring(lastSep+1, id.length).trim
          val text2 = text.trim.replaceAll("""\s*\n\s*""", " ")
          (id2, text2)
      }
      println(s"Writing output to: $output")
      files_contents.saveAsTextFile(output)

    } finally {
      sc.stop()
    }
  }
}