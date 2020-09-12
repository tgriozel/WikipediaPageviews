package code.job

sealed trait JobError { val message: String }

case class JobConfigError(override val message: String) extends JobError

case class JobArgsError(override val message: String) extends JobError

case class InputError(override val message: String) extends JobError

case class OutputError(override val message: String) extends JobError
