package code.service

import java.time.LocalDateTime

import cats.effect.IO
import code.repository.FileSystemRepository
import org.mockito.scalatest.IdiomaticMockito
import org.scalatest.{FunSpec, Matchers}

class StatsOutputServiceSpec extends FunSpec with Matchers with IdiomaticMockito {

  describe("StatsOutputService") {
    val outLocation = "outLocation"
    val fileNamePattern = "yyyyMMddHH'.csv'"
    val dateTime = LocalDateTime.of(2020, 1, 1, 1, 0)
    val literalFileName = "2020010101.csv"

    describe("isCalculationRequired") {
      it ("returns true if the underlying repository says the corresponding file does not exist") {
        // arrange
        val repository = mock[FileSystemRepository]
        repository.fileExistsInDirectory(outLocation, literalFileName) returns false
        val statsOutputService = new StatsOutputService(repository)
        // act
        val result = statsOutputService.isCalculationRequired(outLocation, fileNamePattern, dateTime)
        // assert
        result shouldBe true
      }

      it ("returns false if the underlying repository says the corresponding file does exists") {
        // arrange
        val repository = mock[FileSystemRepository]
        repository.fileExistsInDirectory(outLocation, literalFileName) returns true
        val statsOutputService = new StatsOutputService(repository)
        // act
        val result = statsOutputService.isCalculationRequired(outLocation, fileNamePattern, dateTime)
        // assert
        result shouldBe false
      }
    }

    describe("outputStats") {
      it ("invokes the correct underlying repository method with the right arguments") {
        // arrange
        val statsData = Vector("data")
        val outputPathIO = IO.pure("output")
        val repository = mock[FileSystemRepository]
        repository.writeFile(outLocation, literalFileName, statsData) returns outputPathIO
        val statsOutputService = new StatsOutputService(repository)
        // act
        val result = statsOutputService.outputStats(outLocation, fileNamePattern, dateTime, statsData)
        // assert
        result shouldBe outputPathIO
      }
    }
  }

}
