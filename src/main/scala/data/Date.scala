package data

/** Year, Month, and Day */
case class YMD(
  year:  Int,
  month: Int,
  day:   Int)

object YMD {
  /** "MM/DD/YYYY" format, but actually splits on any non-digit delimiter. */
  def parse(ymdString: String): Option[YMD] = try {
    ymdString.split("""\s*[^\d]+\s*""") match {
      case Array(month, day, year) => Some(YMD(year.toInt, month.toInt, day.toInt))
      case _ =>
        Console.err.println(s"YMD: Bad string, $ymdString")
        None
    }
  } catch {
    case nfe: NumberFormatException =>
      Console.err.println(s"YMD: At least one integer string failed to parse: $ymdString")
      None
  }
}

/** Hour, Minute, and Second */
case class HMS(
  hour:   Int,
  minute: Int,
  second: Int)

object HMS {
  /**
   * "HH:MM[:SS]" format, but actually splits on any non-digit delimiter.
   * Seconds are optional (default to 0).
   */
  def parse(hmsString: String): Option[HMS] = try {
    hmsString.split("""\s*[^\d]+\s*""") match {
      case Array(hour, minute, second) => Some(HMS(hour.toInt, minute.toInt, second.toInt))
      case Array(hour, minute)         => Some(HMS(hour.toInt, minute.toInt, 0))
      case _ =>
        Console.err.println(s"HMS: Bad string, $hmsString")
        None
    }
  } catch {
    case nfe: NumberFormatException =>
      Console.err.println(s"HMS: At least one integer string failed to parse: $hmsString")
      None
  }
}

/** A Date consisting of YMD and HMS. */
case class Date(
  ymd:   YMD,
  hms:   HMS)

object Date {
  /**
   * "YMD HMS" format, but actually splits on any whitespace. Hence, if the
   * string uses whitespace in the YMD or HMS, it will fail to parse.
   */
  def parse(dateString: String): Option[Date] = try {
    dateString.split("""\s+""") match {
      case Array(ymdStr, hmsStr) =>
        for {
          ymd <- YMD.parse(ymdStr)
          hms <- HMS.parse(hmsStr)
        } yield Date(ymd, hms)
      case _ =>
        Console.err.println(s"Date: Bad string, $dateString")
        None
    }
  } catch {
    case nfe: NumberFormatException =>
      Console.err.println(s"Date: At least one integer string failed to parse: $dateString")
      None
  }
}
