package code.job

import java.time.LocalDateTime

import cats.data.EitherT
import cats.effect.IO
import cats.implicits._
import com.google.inject.{Inject, Singleton}
import com.typesafe.scalalogging.{LazyLogging, Logger}
import code._
import code.config.{JobConfig, TimeRange}
import code.repository.HttpDataRepository
import code.service._
import org.apache.spark.sql.DataFrame

import scala.util.Try

@Singleton
class Job @Inject()(jobConfigTry: Try[JobConfig],
                    timeRangeTry: Try[TimeRange],
                    timeSpanService: TimeSpanService,
                    statsOutputService: StatsOutputService,
                    urlService: UrlService,
                    dataRepository: HttpDataRepository,
                    sparkHelper: SparkHelper) extends LazyLogging {

  implicit val jobLogger: Logger = logger

  private def handleJobConfig: Either[JobError, JobConfig] =
    jobConfigTry.toEither.leftMap(t => JobConfigError(t.getMessage))

  private def handleTimeRange: Either[JobError, TimeRange] =
    timeRangeTry.toEither.leftMap(t => JobArgsError(t.getMessage))

  private def computeDateTimes(timeRange: TimeRange, config: JobConfig): List[LocalDateTime] =
    timeSpanService.timeRangeToDateTimes(timeRange).filter { dateTime =>
      statsOutputService.isCalculationRequired(config.outLocation, config.outFileNamePattern, dateTime)
    }

  private def buildUrlWithDateTime(dateTime: LocalDateTime, config: JobConfig): (LocalDateTime, String) =
    dateTime -> urlService.buildFullUrl(config.inDataBaseUrl, config.inFileNamePattern, dateTime)

  private def buildBlackList(config: JobConfig): JobEither[DataFrame] =
    EitherT {
      dataRepository
        .dataFromUrl(config.blacklistUrl)
        .flatMap(rawRows => logTap(rawRows, (x: RawRows) => s"Downloaded blacklist of ${x.length} rows"))
        .map(sparkHelper.rawRowsToBlackListDataFrame(_, enforceBroadcast = true))
        .attempt
    }.leftMap(t => InputError(t.getMessage))

  private def buildPageViewsWithDateTime(dateTime: LocalDateTime, url: String): JobEither[(LocalDateTime, DataFrame)] =
    EitherT {
      dataRepository
        .dataFromGzipUrl(url)
        .flatMap(rawRows => logTap(rawRows, (x: RawRows) => s"Downloaded $dateTime data of ${x.length} rows"))
        .map(dateTime -> sparkHelper.rawRowsToPageViewsDataFrame(_))
        .attempt
    }.leftMap(t => InputError(t.getMessage))

  private def buildAndOutputTopN(config: JobConfig, dateTime: LocalDateTime, pageViews: DataFrame, blackList: DataFrame)
  : JobEither[String] = {
    val topN = sparkHelper.computeTopNPageViews(config.topNCount, pageViews, blackList)
    val topNRows = sparkHelper.pageViewsToCsvLines(topN)
    EitherT {
      logTap(topNRows, (x: RawRows) => s"Computed topN of ${x.length} rows for $dateTime")
        .flatMap(statsOutputService.outputStats(config.outLocation, config.outFileNamePattern, dateTime, _))
        .attempt
    }.leftMap(t => OutputError(t.getMessage))
  }

  def generateAndOutputTopNs: JobEither[JobOutput] = {
    EitherT.fromEither[IO] {
      for {
        timeRange <- handleTimeRange
        config <- handleJobConfig
      } yield (config, computeDateTimes(timeRange, config).map(buildUrlWithDateTime(_, config)))
    }.flatMap { case (config, dateTimeAndUrls) =>
      for {
        blackList <- buildBlackList(config)
        outputPaths <- {
          dateTimeAndUrls
            .map { case (dateTime, url) =>
              buildPageViewsWithDateTime(dateTime, url)
            }
            .map { pageViewsWithDateTimeEither =>
              for {
                pageViewsWithDateTime <- pageViewsWithDateTimeEither
                output <- {
                  val (dateTime, pageViews) = pageViewsWithDateTime
                  buildAndOutputTopN(config, dateTime, pageViews, blackList)
                }
              } yield output
            }.sequence
        }
      } yield outputPaths
    }.map(JobOutput)
  }

}
