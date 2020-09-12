package code.service

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import cats.effect.IO
import com.google.inject.{Inject, Singleton}
import code.RawRows
import code.repository.FileSystemRepository

@Singleton
class StatsOutputService @Inject()(fileSystemRepo: FileSystemRepository) {

  private def fileName(fileNamePattern: String, dateTime: LocalDateTime): String = {
    s"${DateTimeFormatter.ofPattern(fileNamePattern).format(dateTime)}"
  }

  def isCalculationRequired(outLocation: String, fileNamePattern: String, dateTime: LocalDateTime): Boolean = {
    val targetFileName = fileName(fileNamePattern, dateTime)
    !fileSystemRepo.fileExistsInDirectory(outLocation, targetFileName)
  }

  def outputStats(outLocation: String, fileNamePattern: String, dateTime: LocalDateTime, data: RawRows): IO[String] = {
    val targetFileName = fileName(fileNamePattern, dateTime)
    fileSystemRepo.writeFile(outLocation, targetFileName, data)
  }

}
