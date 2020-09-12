package code.service

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.google.inject.Singleton

@Singleton
class UrlService {

  def buildFullUrl(urlBase: String, filePattern: String, dateTime: LocalDateTime): String =
    s"$urlBase${DateTimeFormatter.ofPattern(filePattern).format(dateTime)}"

}
