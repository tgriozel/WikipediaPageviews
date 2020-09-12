package code.repository

import java.io.{BufferedWriter, File, FileWriter}

import cats.effect.IO
import com.google.inject.Singleton
import code.RawRows

@Singleton
class FileSystemRepository {

  private val FileSeparator = File.separator

  private def outDirectory(outLocation: String): File = {
    new File(outLocation) match {
      case dir if dir.isDirectory =>
        dir
      case dir =>
        dir.mkdir()
        dir
    }
  }

  def fileExistsInDirectory(directory: String, targetFileName: String): Boolean = {
    outDirectory(directory)
      .listFiles
      .filter(_.isFile)
      .map(_.getName)
      .contains(targetFileName)
  }

  def writeFile(directory: String, targetFileName: String, data: RawRows): IO[String] = {
    val filePath = s"$directory$FileSeparator$targetFileName"
    IO {
      val file = new File(filePath)
      if (!file.exists())
        file.createNewFile()
      if (!file.canWrite)
        file.setWritable(true)
      val writer = new BufferedWriter(new FileWriter(file))
      data.foreach { r =>
        writer.write(r)
        writer.newLine()
      }
      writer.close()
    }.map(_ => filePath)
  }

}
