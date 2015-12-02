package course2.module5

import org.apache.spark.{SparkConf, SparkContext}

/** Inverted Index - Basis of Search Engines */
object InvertedIndex {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("InvertedIndex")
    conf.set("spark.app.id", "InvertedIndex")   // To silence Metrics warning.

    val input = "output/crawl"
    val output = "output/index-index"
    val sc = new SparkContext(conf)

    try {
      // Load the input "crawl" data, where each line has the format:
      //   (document_id, text)
      // First remove the outer parentheses, split on the first comma,
      // trim whitespace from the name (we'll do it later for the text)
      // and convert the text to lower case.
      // NOTE: The args("input-path") is a directory; Spark finds the correct
      // data files, part-NNNNN.
      val lineRE = """^\s*\(([^,]+),(.*)\)\s*$""".r
      val inputRDD = sc.textFile(input) map {
        case lineRE(name, text) => (name.trim, text.toLowerCase)
        case badLine =>
          Console.err.println(s"Unexpected line: $badLine")
          // If any of these were returned, you could filter them out below.
          ("", "")
      }

      val now = System.currentTimeMillis()
      val out = s"${output}-$now"
      // Split on non-alphanumeric sequences of character as before.
      // Rather than map to "(word, 1)" tuples, we treat the words by values
      // and count the unique occurrences.
      inputRDD
        .flatMap {
          case (path, text) =>
            // If we don't trim leading whitespace, the regex split creates
            // an undesired leading "" word! Also, setup the (word,path) as
            // the key for reducing, and an initial count of 1.
            // Use a refined regex to retain abbreviations, e.g., "there's".
            text.trim.split("""[^\w']""") map (word => ((word, path), 1))
        }
        .reduceByKey{
          (count1, count2) => count1 + count2
        }
        .map {
          case ((word, path), n) => (word, (path, n))
        }
        .groupByKey  // The words are the keys
        .map {
          case (word, iterable) => (word, iterable.mkString(", "))
        }
        .saveAsTextFile(out)
    } finally {
      sc.stop()
    }
  }
}

