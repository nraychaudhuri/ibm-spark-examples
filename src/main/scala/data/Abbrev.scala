package data

/**
 * A case class for the abbreviations data: <code>book,book_name</code>.
 */
case class Abbrev(book: String, bookName: String)

object Abbrev {
  val lineRE = """^\s*([^,]+)\s*\t\s*(.*)$""".r
  def parse(s: String): Option[Abbrev] = s match {
    case lineRE(book, bookName) => Some(Abbrev(book,bookName))
    case line =>
      Console.err.println(s"ERROR: Invalid abbreviations line: $line")
      None
  }
}
