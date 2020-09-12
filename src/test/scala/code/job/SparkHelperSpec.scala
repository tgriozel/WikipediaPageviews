package code.job

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.Row
import org.scalatest.{FunSpec, Matchers}

class SparkHelperSpec extends FunSpec with Matchers with DataFrameSuiteBase {

  describe("SparkHelper") {
    describe("rawRowsToPageViewsDataFrame") {
      it("transforms raw strings to a page views DataFrame, skipping ill-formatted lines") {
        // arrange
        val rawRows = Vector(
          "domain1 page1 1 0",
          "domain2 page2 2 0",
          "incorrect input"
        )
        val helper = new SparkHelper(spark)
        // act
        val result = helper.rawRowsToPageViewsDataFrame(rawRows)
        // assert
        val expectedData = Seq(
          Row("domain1", "page1", 1L),
          Row("domain2", "page2", 2L)
        )
        val expectedDf = spark.createDataFrame(
          spark.sparkContext.parallelize(expectedData),
          SparkHelper.pageViewsSchema
        )
        assertDataFrameEquals(result, expectedDf)
      }
    }

    describe("rawRowsToBlackListDataFrame") {
      it("transforms raw strings to a blacklist DataFrame, skipping ill-formatted lines") {
        // arrange
        val rawRows = Vector(
          "domain1 page1",
          "domain2 page2",
          "an incorrect input"
        )
        val helper = new SparkHelper(spark)
        // act
        val result = helper.rawRowsToBlackListDataFrame(rawRows, enforceBroadcast = false)
        // assert
        val expectedData = Seq(
          Row("domain1", "page1"),
          Row("domain2", "page2")
        )
        val expectedDf = spark.createDataFrame(
          spark.sparkContext.parallelize(expectedData),
          SparkHelper.blackListSchema
        )
        assertDataFrameEquals(result, expectedDf)
      }
    }
  }

  describe("computeTopNPageViews") {
    it("returns the top N page views for each domain, excluding blacklist entries") {
      // arrange
      val pageViewsData = Seq(
        Row("domainA", "page1", 1L),
        Row("domainA", "page2", 2L),
        Row("domainA", "page3", 3L),
        Row("domainB", "page1", 1L),
        Row("domainB", "page2", 2L),
        Row("domainB", "page3", 3L),
        Row("domainC", "page1", 1L),
        Row("domainC", "page2", 2L),
        Row("domainC", "page3", 3L),
        Row("domainD", "page1", 1L),
      )
      val pageViewsDf = spark.createDataFrame(
        spark.sparkContext.parallelize(pageViewsData),
        SparkHelper.pageViewsSchema
      )
      val blackListData = Seq(
        Row("domainA", "page5"),
        Row("domainC", "page3"),
        Row("domainE", "page1")
      )
      val blackListDf = spark.createDataFrame(
        spark.sparkContext.parallelize(blackListData),
        SparkHelper.blackListSchema
      )
      val helper = new SparkHelper(spark)
      // act
      val result = helper.computeTopNPageViews(2, pageViewsDf, blackListDf)
      // assert
      val expectedData = Seq(
        Row("domainA", "page3", 3L),
        Row("domainA", "page2", 2L),
        Row("domainB", "page3", 3L),
        Row("domainB", "page2", 2L),
        Row("domainC", "page2", 2L),
        Row("domainC", "page1", 1L),
        Row("domainD", "page1", 1L),
      )
      val expectedDf = spark.createDataFrame(
        spark.sparkContext.parallelize(expectedData),
        SparkHelper.pageViewsSchema
      )
      assertDataFrameEquals(result, expectedDf)
    }
  }

  describe("pageViewsToCsvLines") {
    it("transforms a page views DataFrame to a csv-ready collection of Strings") {
      // arrange
      val pageViewsData = Seq(
        Row("domain1", "page1", 1L),
        Row("domain2", "page2", 2L)
      )
      val pageViewsDf = spark.createDataFrame(
        spark.sparkContext.parallelize(pageViewsData),
        SparkHelper.pageViewsSchema
      )
      val helper = new SparkHelper(spark)
      // act
      val result = helper.pageViewsToCsvLines(pageViewsDf)
      // assert
      result shouldBe Vector(
        "domain1,page1,1",
        "domain2,page2,2"
      )
    }
  }
}
