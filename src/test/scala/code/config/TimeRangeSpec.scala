package code.config

import java.time.LocalDateTime

import org.scalatest.{FunSpec, Matchers}
import org.scalatest.TryValues._

import scala.util.{Failure, Success}

class TimeRangeSpec extends FunSpec with Matchers {

  describe("TimeRange") {
    describe("apply") {
      it("returns a single hour TimeRange if no argument is passed") {
        // arrange
        val args = Seq.empty[String]
        // act
        val result = TimeRange(args)
        // assert
        result.success.map { timeRange =>
          timeRange.start shouldBe timeRange.finish
        }
      }

      it("returns the TimeRange associated to one argument") {
        // arrange
        val args = Seq("2020010101")
        // act
        val result = TimeRange(args)
        // assert
        val expectedDateTime = LocalDateTime.of(2020, 1, 1, 1, 0)
        result.success shouldBe Success(TimeRange(expectedDateTime, expectedDateTime))
      }

      it("returns the TimeRange associated to two arguments") {
        // arrange
        val args = Seq("2020010101", "2020060601")
        // act
        val result = TimeRange(args)
        // assert
        val expectedDateTimeA = LocalDateTime.of(2020, 1, 1, 1, 0)
        val expectedDateTimeB = LocalDateTime.of(2020, 6, 6, 1, 0)
        result.success shouldBe Success(TimeRange(expectedDateTimeA, expectedDateTimeB))
      }

      it("returns a Failure if the arguments are wrong") {
        // arrange
        val args = Seq("Incorrect input")
        // act
        val result = TimeRange(args)
        // assert
        result.failure shouldBe a[Failure[_]]
      }
    }
  }

}
