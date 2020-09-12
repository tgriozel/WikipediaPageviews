package code.service

import java.time.LocalDateTime

import code.config.TimeRange
import org.scalatest.{FunSpec, Matchers}

class TimeSpanServiceSpec extends FunSpec with Matchers {

  describe("TimeSpanService") {
    describe("timeRangeToDateTimes") {
      it("returns all the hours between the start and finish of a TimeRange") {
        // arrange
        val start = LocalDateTime.of(2020, 1, 1, 1, 0)
        val finish = LocalDateTime.of(2020, 1, 1, 6, 0)
        val timeRange = TimeRange(start, finish)
        val service = new TimeSpanService()
        // act
        val result = service.timeRangeToDateTimes(timeRange)
        // assert
        result shouldBe List(
          LocalDateTime.of(2020, 1, 1, 1, 0),
          LocalDateTime.of(2020, 1, 1, 2, 0),
          LocalDateTime.of(2020, 1, 1, 3, 0),
          LocalDateTime.of(2020, 1, 1, 4, 0),
          LocalDateTime.of(2020, 1, 1, 5, 0),
          LocalDateTime.of(2020, 1, 1, 6, 0)
        )
      }
    }
  }

}
