package course2.module5

import org.apache.spark.ml.feature.{IDFModel, HashingTF, IDF, Tokenizer}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}


//TF = Term frequency
//TF(t) = (Number of times term t appears in a document) / (Total number of terms in the document).
//IDF = Inverse document frequency
//log_e(Total number of documents / Number of documents with term t in it).
//Tf-idf weight is the product of these quantities

object InvertedIndexWithTFIDF {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("TF-IDF")
    // Change to a more reasonable default number of partitions for our data
    // (from 200)
    conf.set("spark.sql.shuffle.partitions", "4")
    conf.set("spark.app.id", "TF-IDF")   // To silence Metrics warning.
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val input = "output/crawl"
    val output = "output/index-index-tf-idf"


    //read the crawl data and create a RDD of (filename, contents of the file)
    val lineRE = """^\s*\(([^,]+),(.*)\)\s*$""".r
    val inputRDD: RDD[(String, String)] = sc.textFile(input) map {
      case lineRE(name, text) => (name.trim, text.toLowerCase)
      case badLine =>
        Console.err.println(s"Unexpected line: $badLine")
        // If any of these were returned, you could filter them out below.
        ("", "")
    }

    val crawlData = sqlContext.createDataFrame(inputRDD)
      .toDF("filename", "contents")

    //A tokenizer that converts the input string to lowercase and then splits it by white spaces.
    val tokenizer = new Tokenizer().setInputCol("contents").setOutputCol("words")
    val wordsData: DataFrame = tokenizer.transform(crawlData)


    //Maps a sequence of terms to their term frequencies using the hashing trick.
    //input data is all the words that we tokenized
    //and the output TF value will be in the rawFeatures column
    val hashingTF: HashingTF = new HashingTF()
      .setInputCol("words")
      .setOutputCol("rawFeatures")
    val tf: DataFrame = hashingTF.transform(wordsData)

    //caching the result as it will be used multiple times by IDF
    tf.cache()

    //input is the TF values and the TF-IDF weight will be available in "features" column
    val idf = new IDF()
      .setInputCol("rawFeatures")
      .setOutputCol("features")
    val idfModel: IDFModel = idf.fit(tf)

    //now that we have the model we can use the tf dataframe to get the final result
    val tfidf: DataFrame = idfModel.transform(tf)
    tfidf.printSchema()


    //now group the result by word where each file will have a tfidf value for the given word
    //TODO: it would be nice to do this in Dataframe
    import org.apache.spark.mllib.linalg._
    val finalRDD: RDD[(String, String)] = tfidf.select("filename", "words", "features").rdd.flatMap {
      case Row(filename, words: Seq[_], v: SparseVector) =>
        //match each word with its tfidf value
        val pairs = words.map(_.toString).zip(v.values.toList)
        pairs.map(t => (t._1, (filename, t._2))) //TODO: there is a possibility that same filename occurs multiple times
    }
    .groupByKey()
    .map {
      case (word, iterable) => (word, iterable.mkString(", "))
    }

    //saving the result to a file
    val now = System.currentTimeMillis()
    val out = s"${output}-$now"
    finalRDD.saveAsTextFile(out)

    sc.stop()
  }
}
