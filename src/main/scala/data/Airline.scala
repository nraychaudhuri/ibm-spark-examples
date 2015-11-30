package data

import scala.math.Ordered

/**
 * Represent flight data from RITA (http://www.rita.dot.gov/), hosted at
 * http://stat-computing.org/dataexpo/2009/the-data.html.
 * Note that the types of delay are described here:
 * http://aspmhelp.faa.gov/index.php/Types_of_Delay
 * For example, "NAS delay" is within the control of the National Airspace System
 * (NAS) and may include: non-extreme weather conditions, airport operations,
 * heavy traffic volume, air traffic control, etc. Delays that occur after
 * actually leaving the gate are usually attributed to the NAS.
 * CRS is the Computer Reservation System.
 * Because case classes in Scala 2.10 are still limited to 22 fields, nested
 * instances of {@link Flight.Date} and {@link Flight.Times} types are used
 * to encapsulate some of the fields.
 * TODO: Note below that {@link Flight.Date} and {@link Flight.Times} types
 * both extend <code>Ordered</code>. <code>Flight</code> should, too.
 */
case class Flight(
                   date:              Flight.Date,  // fields 1-4.
                   times:             Flight.Times, // fields 5-8, 12-16, 20-21
                   uniqueCarrier:     String,       //  9:  unique carrier code
                   flightNum:         Int,          // 10:  flight number
                   tailNum:           String,       // 11:  plane tail number
                   origin:            String,       // 17:  origin IATA airport code
                   dest:              String,       // 18:  destination IATA airport code
                   distance:          Int,          // 19:  in miles
                   canceled:          Int,          // 22:  was the flight canceled?
                   cancellationCode:  String,       // 23:  reason for cancellation (A = carrier, B = weather, C = NAS, D = security)
                   diverted:          Int,          // 24:  1 = yes, 0 = no
                   carrierDelay:      Int,          // 25:  in minutes
                   weatherDelay:      Int,          // 26:  in minutes
                   nasDelay:          Int,          // 27:  in minutes
                   securityDelay:     Int,          // 28:  in minutes
                   lateAircraftDelay: Int           // 29:  in minutes
                   )  // No class body needed. What methods are added automatically by "case"?

object Flight {

  /** Date type used in Flight instances. */
  case class Date(
                   year:              Int,     //  1:  1987-2008
                   month:             Int,     //  2:  1-12
                   dayOfMonth:        Int,     //  3:  1-31
                   dayOfWeek:         Int      //  4:  1 (Monday) - 7 (Sunday)
                   ) extends Ordered[Date] {
    def compare(that: Date): Int = {
      val diffYear = year - that.year
      if (diffYear != 0) diffYear
      else {
        val diffMonth = month - that.month
        if (diffMonth != 0) diffMonth
        else dayOfMonth - that.dayOfMonth
      }
    }

    // Counts from 1 and starts at Monday.
    private val dayOfWeekToString = Vector(
      "", "Monday", "Tuesday", "Wednesday", "Thurday", "Friday", "Saturday", "Sunday")

    override def toString =
      "%4d-%02d-%02d (%s)".format(year, month, dayOfMonth, dayOfWeekToString(dayOfWeek))
  }

  /** Times type used in Flight instances. */
  case class Times(
                    depTime:           Int,     //  5:  actual departure time (local, hhmm)
                    crsDepTime:        Int,     //  6:  scheduled departure time (local, hhmm)
                    arrTime:           Int,     //  7:  actual arrival time (local, hhmm)
                    crsArrTime:        Int,     //  8:  scheduled arrival time (local, hhmm)
                    actualElapsedTime: Int,     // 12:  in minutes
                    crsElapsedTime:    Int,     // 13:  in minutes
                    airTime:           Int,     // 14:  in minutes
                    arrDelay:          Int,     // 15:  arrival delay, in minutes
                    depDelay:          Int,     // 16:  departure delay, in minutes
                    taxiIn:            Int,     // 20:  taxi in time, in minutes
                    taxiOut:           Int      // 21:  taxi out time in minutes
                    ) extends Ordered[Times] {
    /** Only compare the first four fields */
    def compare(that: Times): Int = {
      val diffDepTime = depTime - that.depTime
      if (diffDepTime != 0) diffDepTime
      else {
        val diffCRSDepTime = crsDepTime - that.crsDepTime
        if (diffCRSDepTime != 0) diffCRSDepTime
        else {
          val diffArrTime = arrTime - that.arrTime
          if (diffArrTime != 0) diffArrTime
          else crsArrTime - that.crsArrTime
        }
      }
    }

    // Write as a tuple.
    override def toString =
      s"($depTime,$crsDepTime,$arrTime,$crsArrTime,$actualElapsedTime,$crsElapsedTime,$airTime,$arrDelay,$depDelay,$taxiIn,$taxiOut)"
  }

  // The string fields are NOT quoted.
  def parse(line: String): Option[Flight] = {
    val fields = line.trim.split("""\s*,\s*""")
    try {
      if (fields(0) == "Year") None
      else {
        import Conversions.toInt
        Some(Flight(
          Date(
            toInt(fields(0)),
            toInt(fields(1)),
            toInt(fields(2)),
            toInt(fields(3))),
          Times(
            toInt(fields(4)),
            toInt(fields(5)),
            toInt(fields(6)),
            toInt(fields(7)),
            toInt(fields(11)),
            toInt(fields(12)),
            toInt(fields(13)),
            toInt(fields(14)),
            toInt(fields(15)),
            toInt(fields(19)),
            toInt(fields(20))),
          fields(8).trim,
          toInt(fields(9)),
          fields(10).trim,
          fields(16).trim,
          fields(17).trim,
          toInt(fields(18)),
          toInt(fields(21)),
          fields(22).trim,
          toInt(fields(23)),
          toInt(fields(24)),
          toInt(fields(25)),
          toInt(fields(26)),
          toInt(fields(27)),
          toInt(fields(28))))
      }
    } catch {
      case ex: NumberFormatException =>
        Console.err.println(s"$ex: line = $line")
        None
      case ex: IndexOutOfBoundsException =>
        Console.err.println(s"$ex: line = $line")
        None
    }
  }
}

/**
 * Airport data.
 * @TODO extends <code>Ordered</code>
 */
case class Airport(
                    iata:      String,    // 1: IATA code
                    airport:   String,    // 2: Name
                    city:      String,    // 3: City
                    state:     String,    // 4: State
                    country:   String,    // 5: Country
                    latitude:  Float,     // 6: Latitude
                    longitude: Float      // 7: Longitude
                    )

object Airport {
  // All the string fields are quoted, including in the header row, UNLESS
  // the value is NA. There is still one entry we don't handle correctly,
  // where the name is "W. H. \"Bud\" Barron ", due to the escaped quotes.
  import Conversions.toInt
  val headerRE = """^\s*"iata"\s*,.*""".r
  val lineRE = """^\s*"([^"]+)"\s*,\s*"([^"]+)"\s*,\s*"?([^"]+)"?\s*,\s*"?([^"]+)"?\s*,\s*"([^"]+)"\s*,\s*([-.\d]+)\s*,\s*([-.\d]+)\s*$""".r
  def parse(s: String): Option[Airport] = s match {
    case headerRE() => None
    case lineRE(iata, airport, city, state, country, lat, lng) =>
      Some(Airport(iata.trim, airport.trim, city.trim, state.trim, country.trim, lat.toFloat, lng.toFloat))
    case line =>
      Console.err.println(s"ERROR: Invalid Airport line: $line")
      None
  }
}

/**
 * Carrier data.
 * @TODO extends <code>Ordered</code>
 */
case class Carrier(
                    code:        String,  // 1: Airline code, e.g., UA for United Airlines
                    description: String   // 2: Full name
                    )

object Carrier {
  // The two string fields are quoted, but NOT including the header row.
  val headerRE = """^\s*Code\s*,.*""".r
  val lineRE = """^\s*"([^"]+)"\s*,\s*"([^"]+)"\s*$""".r
  def parse(line: String): Option[Carrier] = line match {
    case headerRE() => None
    case lineRE(code, desc) => Some(Carrier(code.trim, desc.trim))
    case line =>
      Console.err.println(s"ERROR: Invalid Carrier line: $line")
      None
  }
}

/**
 * Plane data.
 * @TODO extends <code>Ordered</code>
 */
case class Plane(
                  tailNum:      String,  // 1: E.g., N12345
                  kind:         String,  // 2: Usually "Corporation"
                  manufacturer: String,  // 3: E.g., Boeing
                  issueDate:    String,  // 4: MM/DD/YYYY
                  model:        String,  // 5: E.g., 767-201
                  status:       String,  // 6: Usually "Valid"
                  aircraftType: String,  // 7: Usually "Fixed Wing Multi-Engine"
                  engineType:   String,  // 8: E.g., "Turbo-Fan" or "Turbo-Jet"
                  year:         Int      // 9: YYYY
                  )

object Plane {
  // The string fields are NOT quoted.
  val headerRE   = """^\s*tailnum\s*,.*""".r
  val tailOnlyRE = """^\s*(N\w+)\s*$""".r
  val lineRE     = """^\s*(N[^,]+)\s*,\s*([^,]+)\s*,\s*([^,]+)\s*,\s*([^,]+)\s*,\s*([^,]+)\s*,\s*([^,]+)\s*,\s*([^,]+)\s*,\s*([^,]+)\s*,\s*(\S+)\s*$""".r
  def parse(line: String): Option[Plane] = line match {
    case headerRE() => None
    case tailOnlyRE(tail) =>
      Some(Plane(tail, "", "", "", "", "", "", "", 0))
    case lineRE(tail, kind, man, iss, mod, stat, air, eng, yr) =>
      val year = if (yr == "None") -1 else Conversions.toInt(yr)
      Some(Plane(tail, kind, man, iss, mod, stat, air, eng, year))
    case line =>
      Console.err.println(s"ERROR: Invalid Plane line: $line")
      None
  }
}
