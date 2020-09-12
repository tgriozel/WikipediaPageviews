package code.config

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

import scala.util.Try

case class TimeRange(start: LocalDateTime, finish: LocalDateTime)

object TimeRange {

  private val formatter = DateTimeFormatter.ofPattern("yyyyMMddHH")

  private def default: TimeRange = {
    val currentDateTime = LocalDateTime.now().truncatedTo(ChronoUnit.HOURS)
    val startDateTime = currentDateTime.minusHours(24)
    TimeRange(startDateTime, currentDateTime)
  }

  private def fromString(dateTimeStr: String): Try[TimeRange] = {
    Try(LocalDateTime.parse(dateTimeStr, formatter)).map { dateTime =>
      TimeRange(dateTime, dateTime)
    }
  }

  private def fromStrings(dateTimeStrA: String, dateTimeStrB: String): Try[TimeRange] = {
    for {
      dateTimeA <- Try(LocalDateTime.parse(dateTimeStrA, formatter))
      dateTimeB <- Try(LocalDateTime.parse(dateTimeStrB, formatter))
    } yield if (dateTimeA.isBefore(dateTimeB)) TimeRange(dateTimeA, dateTimeB) else TimeRange(dateTimeB, dateTimeA)
  }

  def apply(args: Seq[String]): Try[TimeRange] = args match {
    case Nil => Try(default)
    case head :: Nil => fromString(head)
    case head :: tail => fromStrings(head, tail.head)
  }

}
