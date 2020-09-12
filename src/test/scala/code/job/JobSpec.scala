package code.job

import java.time.LocalDateTime

import cats.effect.IO
import code.config.{JobConfig, TimeRange}
import code.repository.HttpDataRepository
import code.service.{StatsOutputService, TimeSpanService, UrlService}
import org.apache.spark.sql.DataFrame
import org.mockito.scalatest.IdiomaticMockito
import org.scalatest.{FunSpec, Matchers}

import scala.util.Try

class JobSpec extends FunSpec with Matchers with IdiomaticMockito {

  describe("Job") {
    describe("generateAndOutputTopNs") {
      it("does the expected interactions with its dependencies under normal circumstances") {
        // arrange
        val conf = JobConfig(
          topNCount = 1,
          blacklistUrl = "blacklistUrl",
          inDataBaseUrl = "inDataBaseUrl",
          inFileNamePattern = "inFileNamePattern",
          outLocation = "outLocation",
          outFileNamePattern = "outFileNamePattern"
        )
        val path = "path"
        val pageViewsUrl = "pageViewsUrl"
        val argDateTime = LocalDateTime.now()
        val timeRange = TimeRange(argDateTime, argDateTime)
        val dateTimeList = List(argDateTime)
        val blackListRows = Vector("blackListRows")
        val pageViewRows = Vector("pageViewRows")
        val topNRows = Vector("topNRows")
        val blackList = mock[DataFrame]
        val pageViews = mock[DataFrame]
        val topN = mock[DataFrame]
        val jobConfigTry = Try(conf)
        val timeRangeTry = Try(timeRange)
        val timeSpanService = mock[TimeSpanService]
        timeSpanService.timeRangeToDateTimes(timeRange) returns dateTimeList
        val statsOutputService = mock[StatsOutputService]
        statsOutputService.isCalculationRequired(conf.outLocation, conf.outFileNamePattern, argDateTime) returns true
        statsOutputService.outputStats(conf.outLocation, conf.outFileNamePattern, argDateTime, topNRows) returns IO(path)
        val urlService = mock[UrlService]
        urlService.buildFullUrl(conf.inDataBaseUrl, conf.inFileNamePattern, argDateTime) returns pageViewsUrl
        val dataRepository = mock[HttpDataRepository]
        dataRepository.dataFromUrl(conf.blacklistUrl) returns IO(blackListRows)
        dataRepository.dataFromGzipUrl(pageViewsUrl) returns IO(pageViewRows)
        val sparkHelper = mock[SparkHelper]
        sparkHelper.rawRowsToBlackListDataFrame(blackListRows, enforceBroadcast = true) returns blackList
        sparkHelper.rawRowsToPageViewsDataFrame(pageViewRows) returns pageViews
        sparkHelper.computeTopNPageViews(conf.topNCount, pageViews, blackList) returns topN
        sparkHelper.pageViewsToCsvLines(topN) returns topNRows
        val job = new Job(
          jobConfigTry,
          timeRangeTry,
          timeSpanService,
          statsOutputService,
          urlService,
          dataRepository,
          sparkHelper
        )
        // act
        val result = job.generateAndOutputTopNs.value.unsafeRunSync()
        // assert
        result shouldBe Right(JobOutput(List(path)))
      }

      it("returns a Left of the precise Error that happened (JobConfigError)") {
        // arrange
        val jobConfigTry = Try(throw new Exception(""))
        val timeRangeTry = Try(mock[TimeRange])
        val timeSpanService = mock[TimeSpanService]
        val statsOutputService = mock[StatsOutputService]
        val urlService = mock[UrlService]
        val dataRepository = mock[HttpDataRepository]
        val sparkHelper = mock[SparkHelper]
        val job = new Job(
          jobConfigTry,
          timeRangeTry,
          timeSpanService,
          statsOutputService,
          urlService,
          dataRepository,
          sparkHelper
        )
        // act
        val result = job.generateAndOutputTopNs.value.unsafeRunSync()
        // assert
        result shouldBe a[Left[JobConfigError, _]]
      }

      it("returns a Left of the precise Error that happened (JobArgsError)") {
        // arrange
        val jobConfigTry = Try(mock[JobConfig])
        val timeRangeTry = Try(throw new Exception(""))
        val timeSpanService = mock[TimeSpanService]
        val statsOutputService = mock[StatsOutputService]
        val urlService = mock[UrlService]
        val dataRepository = mock[HttpDataRepository]
        val sparkHelper = mock[SparkHelper]
        val job = new Job(
          jobConfigTry,
          timeRangeTry,
          timeSpanService,
          statsOutputService,
          urlService,
          dataRepository,
          sparkHelper
        )
        // act
        val result = job.generateAndOutputTopNs.value.unsafeRunSync()
        // assert
        result shouldBe a[Left[JobArgsError, _]]
      }


      it("returns a Left of the precise Error that happened (InputError)") {
        // arrange
        val conf = JobConfig(
          topNCount = 1,
          blacklistUrl = "blacklistUrl",
          inDataBaseUrl = "inDataBaseUrl",
          inFileNamePattern = "inFileNamePattern",
          outLocation = "outLocation",
          outFileNamePattern = "outFileNamePattern"
        )
        val pageViewsUrl = "pageViewsUrl"
        val argDateTime = LocalDateTime.now()
        val timeRange = TimeRange(argDateTime, argDateTime)
        val dateTimeList = List(argDateTime)
        val jobConfigTry = Try(conf)
        val timeRangeTry = Try(timeRange)
        val timeSpanService = mock[TimeSpanService]
        timeSpanService.timeRangeToDateTimes(timeRange) returns dateTimeList
        val statsOutputService = mock[StatsOutputService]
        statsOutputService.isCalculationRequired(conf.outLocation, conf.outFileNamePattern, argDateTime) returns true
        val urlService = mock[UrlService]
        urlService.buildFullUrl(conf.inDataBaseUrl, conf.inFileNamePattern, argDateTime) returns pageViewsUrl
        val dataRepository = mock[HttpDataRepository]
        dataRepository.dataFromUrl(conf.blacklistUrl) returns IO(throw new Exception(""))
        val sparkHelper = mock[SparkHelper]
        val job = new Job(
          jobConfigTry,
          timeRangeTry,
          timeSpanService,
          statsOutputService,
          urlService,
          dataRepository,
          sparkHelper
        )
        // act
        val result = job.generateAndOutputTopNs.value.unsafeRunSync()
        // assert
        result shouldBe a[Left[InputError, _]]
      }
    }
  }
}
