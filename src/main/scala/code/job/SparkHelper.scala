package code.job

import com.google.inject.{Inject, Singleton}
import code.RawRows
import org.apache.spark.sql.catalyst.plans.LeftAnti
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.util.Try

@Singleton
class SparkHelper @Inject()(session: SparkSession) extends Serializable{

  import SparkHelper._

  def rawRowsToPageViewsDataFrame(rawData: RawRows): DataFrame = {
    val rawRdd = session.sparkContext.parallelize(rawData)
    val parsedRdd = rawRdd.flatMap(_.split(ColumnsSeparator).toList match {
      case domain :: page :: viewCount :: _ :: Nil =>
        Try(viewCount.toLong).map(Row(domain, page, _)).toOption
      case _ =>
        None
    })
    session.createDataFrame(parsedRdd, pageViewsSchema)
  }

  def rawRowsToBlackListDataFrame(rawData: RawRows, enforceBroadcast: Boolean): DataFrame = {
    val rawRdd = session.sparkContext.parallelize(rawData)
    val parsedRdd = rawRdd.flatMap(_.split(ColumnsSeparator).toList match {
      case domain :: page :: Nil =>
        Some(Row(domain, page))
      case _ =>
        None
    })
    val dataFrame = session.createDataFrame(parsedRdd, blackListSchema)
    if (enforceBroadcast) broadcast(dataFrame) else dataFrame
  }

  def computeTopNPageViews(n: Int, pageViews: DataFrame, blackList: DataFrame): DataFrame = {
    val window = Window.partitionBy(col(DomainColumn)).orderBy(col(ViewCountColumn).desc)
    pageViews
      .join(blackList, Seq(DomainColumn, PageColumn), LeftAnti.toString)
      .withColumn(RowNumberColumn, row_number() over window)
      .where(col(RowNumberColumn).<=(lit(n)))
      .select(DomainColumn, PageColumn, ViewCountColumn)
      .orderBy(col(DomainColumn).asc, col(ViewCountColumn).desc, col(PageColumn).asc)
  }

  def pageViewsToCsvLines(pageViews: DataFrame): RawRows = {
    val rows = pageViews.rdd.map(row => s"${row(0)},${row(1)},${row(2)}")
    rows.collect().toVector
  }

}

object SparkHelper {

  private val ColumnsSeparator = " "
  private val DomainColumn = "domain"
  private val PageColumn = "page"
  private val ViewCountColumn = "view_count"
  private val RowNumberColumn = "row_number"

  val pageViewsSchema: StructType = new StructType()
    .add(StructField(DomainColumn, StringType))
    .add(StructField(PageColumn, StringType))
    .add(StructField(ViewCountColumn, LongType))

  val blackListSchema: StructType = new StructType()
    .add(StructField(DomainColumn, StringType))
    .add(StructField(PageColumn, StringType))

}
