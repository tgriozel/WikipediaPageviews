import cats.data.EitherT
import cats.effect.IO
import com.typesafe.scalalogging.Logger
import code.job.JobError

package object code {

  type RawRows = Vector[String]

  type JobEither[T] = EitherT[IO, JobError, T]

  def logTap[T](thing: T, logFn: T => String)(implicit logger: Logger): IO[T] =
    IO(logger.warn(logFn(thing))).map(_ => thing)

}
