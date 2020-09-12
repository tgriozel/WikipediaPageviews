package code.injection

import com.google.inject.{AbstractModule, Inject, Provides}
import com.typesafe.config.{Config, ConfigFactory}
import code.config.JobConfig
import org.apache.spark.sql.SparkSession

import scala.util.Try

class StaticModule extends AbstractModule {

  override def configure(): Unit = {}

  @Provides
  def providesConfig(): Config = ConfigFactory.load()

  @Provides
  @Inject
  def providesJobConfigTry(config: Config): Try[JobConfig] = JobConfig(config)

  @Provides
  def providesSparkSession(): SparkSession =
    SparkSession
      .builder()
      .config("spark.app.name", "wikipedia-pageviews")
      .config("spark.master", "local[*]")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "localhost")
      .config("spark.local.ip", "localhost")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

}
