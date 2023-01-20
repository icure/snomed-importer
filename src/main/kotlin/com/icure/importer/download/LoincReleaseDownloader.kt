package com.icure.importer.download

import java.io.File
import java.math.BigInteger
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.file.Paths
import java.util.*

class LoincReleaseDownloader(
    private val baseFolder: String
) {
    private val downloadedFileName = "loinc-latest.zip"
    private val client = HttpClient.newHttpClient()

    init {
        File(baseFolder).let {
            if(it.exists()) it.deleteRecursively()
        }
        File(baseFolder).mkdir()
    }

    private fun getSessionCookie(username: String, password: String): String? {
        val formBody = mapOf("log" to username, "pwd" to password)
        val request = HttpRequest.newBuilder()
            .uri(URI.create("https://loinc.org/wp-login.php"))
            .postMultipartFormData(BigInteger(35, Random()).toString(), formBody)
            .build()

        val response = client.send(request, HttpResponse.BodyHandlers.discarding())
        return response.headers().map()["set-cookie"]?.map {
            it.split(';')[0]
        }?.firstOrNull {
            it.startsWith("wordpress_sec")
        }
    }

    fun downloadRelease(username: String, password: String) = getSessionCookie(username,password)
        ?.let {
            val formBody = mapOf("tc_accepted" to "1", "tc_submit" to "Download")

            val request = HttpRequest.newBuilder()
                .uri(URI.create("https://loinc.org/download/loinc-complete/"))
                .header("Cookie", it)
                .postMultipartFormData(BigInteger(35, Random()).toString(), formBody)
                .build()

            client.send(request, HttpResponse.BodyHandlers.ofFile(Paths.get("$baseFolder/$downloadedFileName")))
            val process = ProcessBuilder("unzip", downloadedFileName)
            process.directory(File(baseFolder))
            process.start().waitFor()
        }
}