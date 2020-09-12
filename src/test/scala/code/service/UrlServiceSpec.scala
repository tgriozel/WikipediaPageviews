package code.service

import java.time.LocalDateTime

import org.scalatest.{FunSpec, Matchers}

class UrlServiceSpec extends FunSpec with Matchers {

  describe("UrlService") {
    describe("buildFullUrl") {
      it("build the correct full url") {
        // arrange
        val urlBase = "https://www.test.com/files/"
        val filePattern = "YYYY/YYYY-MM/'stats'-YYYYMMdd-HH0000'.gz'"
        val dateTime = LocalDateTime.of(2020, 1, 1, 1, 0)
        val service = new UrlService()
        // act
        val result = service.buildFullUrl(urlBase, filePattern, dateTime)
        // assert
        result shouldBe "https://www.test.com/files/2020/2020-01/stats-20200101-010000.gz"
      }
    }
  }

}
