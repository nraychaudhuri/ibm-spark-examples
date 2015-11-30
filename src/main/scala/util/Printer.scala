package util

import java.io._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

/**
 * RDD and DataFrame printing utility.
 * The reason this is an object and `out` is passed in with every apply call
 * is because `Printer` is used in classes where a `var out:PrintStream` is
 * reset in test subclasses, so instantiating a Printer instance with the
 * correct `out` instance is tricky.
 */
object Printer {

  /**
   * Print the DataFrame after taking the first n elements.
   */
  def apply(out: PrintStream, msg: String, df: DataFrame, n: Int = 100): Unit =
    apply(out, msg, df.rdd, n)

  /**
   * Print the RDD after taking the first 100 elements.
   * Because this function is overloaded, only one version can
   * have default arguments.
   */
  def print[T](out: PrintStream, msg: String, rdd: RDD[T]): Unit =
    apply(out, msg, rdd, 100)

  /**
   * Print the RDD after taking the first n elements.
   * Because this function is overloaded, only one version can
   * have default arguments.
   */
  def apply[T](out: PrintStream, msg: String, rdd: RDD[T], n: Int): Unit = {
    out.println(s"$msg: (size = ${rdd.count})")
    rdd.take(n) foreach (record => out.println(record))
  }
}