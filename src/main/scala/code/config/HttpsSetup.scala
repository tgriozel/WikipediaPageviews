package code.config

import javax.net.ssl._
import java.security.cert.X509Certificate

object HttpsSetup {

  def setupPermissiveTrustManager(): Unit = {
    val trustAllCerts = Array[TrustManager](new X509TrustManager() {
      override def getAcceptedIssuers: Array[X509Certificate] = null
      override def checkClientTrusted(certs: Array[X509Certificate], s: String): Unit = {}
      override def checkServerTrusted(certs: Array[X509Certificate], s: String): Unit = {}
    })
    val sslContext = SSLContext.getInstance("SSL")
    sslContext.init(null, trustAllCerts, new java.security.SecureRandom())
    HttpsURLConnection.setDefaultSSLSocketFactory(sslContext.getSocketFactory)
  }

}
