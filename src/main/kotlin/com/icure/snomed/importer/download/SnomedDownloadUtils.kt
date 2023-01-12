package com.icure.snomed.importer.download

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.fasterxml.jackson.module.kotlin.SingletonSupport
import com.fasterxml.jackson.module.kotlin.readValue
import java.io.File
import java.math.BigInteger
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.charset.StandardCharsets
import java.nio.file.Paths
import java.security.MessageDigest
import java.util.*
import kotlin.collections.ArrayList

data class ReleaseFolders (
    val delta: String?,
    val snapshot: String?
)

data class ReleaseFile (
    val releaseFileId: Int,
    val label: String,
    val createdAt: String,
    val clientDownloadUrl: String
)

data class ReleaseVersion (
    val releaseVersionId: Int,
    val createdAt: String,
    val name: String,
    val description: String,
    val online: Boolean,
    val publishedAt: String,
    val releaseFiles: List<ReleaseFile>
) {
    fun getPackageMD5(): String? {
        return "RF2 package:[^a-f\\d]+([a-f\\d]{32})".toRegex().find(description)?.destructured?.toList()?.firstOrNull()
    }

    fun getRF2(): ReleaseFile? {
        return releaseFiles.firstOrNull {
            "SnomedCT.*zip".toRegex().matches(it.label)
        }
    }
}

data class Member (
    val memberId: Int,
    val key: String,
    val createdAt: String,
    val licenseName: String,
    val licenseVersion: String,
    val name: String?,
    val staffNotificationEmail: String?,
    val promotePackages: Boolean
)

data class Release (
    val releasePackageId: String,
    val createdAt: String,
    val member: Member,
    val name: String,
    val description: String?,
    val priority: Int,
    val releaseVersions: List<ReleaseVersion>
) {
    fun getLatestRelease(): ReleaseVersion? {
        return releaseVersions
            .maxByOrNull { it.publishedAt }
    }
}

class SnomedReleaseDownloader(
    private val baseFolder: String
) {
    private val client = HttpClient.newHttpClient()

    companion object {
        private val objectMapper: ObjectMapper = ObjectMapper().registerModule(
            KotlinModule.Builder()
                .nullIsSameAsDefault(nullIsSameAsDefault = false)
                .reflectionCacheSize(reflectionCacheSize = 512)
                .nullToEmptyMap(nullToEmptyMap = false)
                .nullToEmptyCollection(nullToEmptyCollection = false)
                .singletonSupport(singletonSupport = SingletonSupport.DISABLED)
                .strictNullChecks(strictNullChecks = false)
                .build()
        )
    }

    fun getSnomedReleases(releaseCode: Int): Release {
        // Release code is 167 for international and 190440 for Belgium
        val request = HttpRequest.newBuilder()
            .uri(URI.create("https://mlds.ihtsdotools.org/api/releasePackages/$releaseCode"))
            .GET()
            .build()

        val response = client.send(request, HttpResponse.BodyHandlers.ofString())
        return objectMapper.readValue(response.body())
    }

    private fun getSessionCookie(username: String, password: String): String? {
        val formBody = mapOf("j_username" to username, "j_password" to password)
        val request = HttpRequest.newBuilder()
            .uri(URI.create("https://mlds.ihtsdotools.org/app/authentication"))
            .postMultipartFormData(BigInteger(35, Random()).toString(), formBody)
            .build()

        val response = client.send(request, HttpResponse.BodyHandlers.discarding())
        return response.headers().map()["set-cookie"]?.let {
            it[0].split(';')[0]
        }
    }

    fun downloadRelease(username: String, password: String, release: ReleaseFile) {
        val sessionCookie = getSessionCookie(username, password)
        sessionCookie?.let { cookie ->
            val request = HttpRequest.newBuilder()
                .uri(URI.create("https://mlds.ihtsdotools.org${release.clientDownloadUrl}"))
                .header("Cookie", cookie)
                .GET()
                .build()

            client.send(request, HttpResponse.BodyHandlers.ofFile(Paths.get("$baseFolder/${release.label}")))
        }
    }

    fun checkMD5(release: ReleaseVersion): Boolean {
        return release.getRF2()?.let { releaseFile ->
            val md = MessageDigest.getInstance("MD5")
            File("$baseFolder/${releaseFile.label}").inputStream().use {
                val b = ByteArray(1024)
                var c = it.read(b)
                do {
                    md.update(b, 0, c)
                    c = it.read(b)
                } while(c != -1)
            }
            val calculatedMD5 = BigInteger(1, md.digest()).toString(16).padStart(32, '0')
            release.getPackageMD5() == calculatedMD5
        } ?: false
    }

    fun getReleaseTypes(release: ReleaseFile): ReleaseFolders {
        val process = ProcessBuilder("unzip", release.label)
        process.directory(File(baseFolder))
        process.start().waitFor()
        val deltaDir = if (File("$baseFolder/${release.label.split('.')[0]}/Delta").isDirectory)
            "$baseFolder/${release.label.split('.')[0]}/Delta/Terminology"
        else null
        val snapshotDir = if (File("$baseFolder/${release.label.split('.')[0]}/Snapshot").isDirectory)
            "$baseFolder/${release.label.split('.')[0]}/Snapshot/Terminology"
        else null
        return ReleaseFolders(deltaDir, snapshotDir)
    }

}

