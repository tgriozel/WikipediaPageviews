package code.injection

import com.google.inject.{AbstractModule, Provides}
import code.config.TimeRange

import scala.util.Try

class ArgsModule(args: Seq[String]) extends AbstractModule {

  override def configure(): Unit = {}

  @Provides
  def providesTimeRangeTry: Try[TimeRange] = TimeRange(args)

}
