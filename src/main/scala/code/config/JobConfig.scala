package code.config

import com.typesafe.config.Config

import scala.util.Try

case class JobConfig(
  topNCount: Int,
  blacklistUrl: String,
  inDataBaseUrl: String,
  inFileNamePattern: String,
  outLocation: String,
  outFileNamePattern: String
)

object JobConfig {

  private val JobConfigPrefix = "wikipedia-pageviews"

  def apply(config: Config): Try[JobConfig] =
    for {
      topNCount <- Try(config.getInt(s"$JobConfigPrefix.top-n-count"))
      blacklistUrl <- Try(config.getString(s"$JobConfigPrefix.blacklist-url"))
      inDataBaseUrl <- Try(config.getString(s"$JobConfigPrefix.input-data-base-url"))
      inFileNamePattern <- Try(config.getString(s"$JobConfigPrefix.input-file-name-pattern"))
      outLocation <- Try(config.getString(s"$JobConfigPrefix.output-location"))
      outFileNamePattern <- Try(config.getString(s"$JobConfigPrefix.output-file-name-pattern"))
    } yield JobConfig(topNCount, blacklistUrl, inDataBaseUrl, inFileNamePattern, outLocation, outFileNamePattern)

}
