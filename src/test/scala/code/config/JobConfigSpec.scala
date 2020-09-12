package code.config

import com.typesafe.config.{ConfigException, ConfigFactory}
import org.scalatest.{FunSpec, Matchers}
import org.scalatest.TryValues._

import scala.util.Success

class JobConfigSpec extends FunSpec with Matchers {

  describe("JobConfig") {
    describe("apply") {
      it("parses a Config and returns a correct Success(JobConfig) if it is well-formatted") {
        // arrange
        val topNCount = 25
        val blacklistUrl = "blacklistUrl"
        val inDataBaseUrl = "inDataBaseUrl"
        val inFileNamePattern = "inFileNamePattern"
        val outLocation = "directory"
        val outFileNamePattern = "outFileNamePattern"
        val config = ConfigFactory.parseString(
          s"""
             |wikipedia-pageviews {
             |  top-n-count: $topNCount
             |  blacklist-url = $blacklistUrl
             |  input-data-base-url = $inDataBaseUrl
             |  input-file-name-pattern = $inFileNamePattern
             |  output-location = $outLocation
             |  output-file-name-pattern = $outFileNamePattern
             |}
             |""".stripMargin
        )
        // act
        val result = JobConfig(config)
        // assert
        result.success shouldBe Success(
          JobConfig(topNCount, blacklistUrl, inDataBaseUrl, inFileNamePattern, outLocation, outFileNamePattern)
        )
      }

      it("parses a Config and returns a Failure if the config is not correct") {
        // arrange
        val config = ConfigFactory.parseString("wikipedia-pageviews {}")
        // act
        val result = JobConfig(config)
        // assert
        result.failure.exception shouldBe a[ConfigException]
      }
    }
  }

}
