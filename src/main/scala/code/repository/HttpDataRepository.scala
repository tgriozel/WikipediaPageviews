package code.repository

import java.io.{BufferedInputStream, InputStream}
import java.net.URL
import java.util.zip.GZIPInputStream

import cats.effect.IO
import com.google.inject.Singleton
import code.RawRows
import code.config.HttpsSetup

import scala.io.Source

@Singleton
class HttpDataRepository {

  HttpsSetup.setupPermissiveTrustManager()

  private def dataFromInputStream(iStream: InputStream): RawRows = {
    val data = Source.fromInputStream(iStream).getLines().toVector
    iStream.close()
    data
  }

  def dataFromUrl(urlStr: String): IO[RawRows] =
    IO(new BufferedInputStream(new URL(urlStr).openStream())).map(dataFromInputStream)

  def dataFromGzipUrl(urlStr: String): IO[RawRows] =
    IO(new GZIPInputStream(new BufferedInputStream(new URL(urlStr).openStream))).map(dataFromInputStream)

}
