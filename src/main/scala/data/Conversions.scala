package data

object Conversions {
  /**
   * Handle the case of blank or "NA" strings. Simply return the default value.
   * On a number format exception, print an error and return the default value.
   */
  def toInt(s: String, default: Int = -1): Int = try {
    val s2 = s.trim
    if (s2.length == 0 || s2 == "NA") default
    else s2.toInt
  } catch {
    case nfe: NumberFormatException =>
      Console.err.println(s"""NFE: "$s".toInt""")
      default
  }
}

