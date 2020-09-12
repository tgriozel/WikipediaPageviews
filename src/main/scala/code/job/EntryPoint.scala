package code.job

import cats.effect.{ExitCode, IO, IOApp}
import com.google.inject.Guice
import com.typesafe.scalalogging.LazyLogging
import code.injection.{ArgsModule, StaticModule}

object EntryPoint extends IOApp with LazyLogging {

  override def run(args: List[String]): IO[ExitCode] = {
    val injector = Guice.createInjector(new StaticModule, new ArgsModule(args))
    injector.getInstance(classOf[Job]).generateAndOutputTopNs.value.map {
      case Left(error: JobError) =>
        logger.error(error.message)
        ExitCode.Error
      case Right(jobOutputList: JobOutput) =>
        jobOutputList.paths match {
          case Nil =>
            logger.warn("No new file generated")
          case paths =>
            logger.warn(s"${paths.length} new file(s) generated:")
            paths.foreach(path => logger.info(path))
        }
        ExitCode.Success
    }
  }
}
