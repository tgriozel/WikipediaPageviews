package code.service

import java.time.LocalDateTime
import java.time.temporal.ChronoUnit

import com.google.inject.Singleton
import code.config.TimeRange

@Singleton
class TimeSpanService {

  def timeRangeToDateTimes(timeRange: TimeRange): List[LocalDateTime] = {
    for (x <- 0 to timeRange.start.until(timeRange.finish, ChronoUnit.HOURS).intValue)
      yield timeRange.start.plus(x, ChronoUnit.HOURS)
  }.toList

}
