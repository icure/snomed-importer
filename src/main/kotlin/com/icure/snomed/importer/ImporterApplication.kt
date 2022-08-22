package com.icure.snomed.importer

import io.icure.kraken.client.apis.CodeApi
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.runBlocking
import org.springframework.boot.CommandLineRunner
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import java.io.File

operator fun List<String>.component3() = this[2]
operator fun List<String>.component4() = this[3]
operator fun List<String>.component5() = this[4]
operator fun List<String>.component6() = this[5]
operator fun List<String>.component7() = this[6]
operator fun List<String>.component8() = this[7]
operator fun List<String>.component9() = this[8]
operator fun List<String>.component10() = this[9]

data class SnomedCTCodeUpdate(
    val code: String,
    val region: String,
    val version: String? = null,
    val disabled: Boolean? = null,
    val description: MutableMap<String, String> = mutableMapOf(),
    val synonyms: MutableMap<String, List<String>> = mutableMapOf(),
    val relationsAdd: MutableMap<String, List<String>> = mutableMapOf(),
    val relationsRemove: MutableMap<String, List<String>> = mutableMapOf(),
    val searchTerms: MutableMap<String, Set<String>> = mutableMapOf()
)

@SpringBootApplication
@ExperimentalCoroutinesApi
@ExperimentalStdlibApi
class ImporterApplication : CommandLineRunner {

    override fun run(args: Array<String>) {
        val basePath = System.getenv("BASE_PATH")!!
        val userName = System.getenv("ICURE_USER")!!
        val password = System.getenv("ICURE_PWD")!!
        val iCureUrl = System.getenv("ICURE_URL")!!
        val codeType = System.getenv("CODE_TYPE") ?: "SNOMED"
        val snomedUserName = System.getenv("SNOMED_USER")!!
        val snomedPassword = System.getenv("SNOMED_PWD")!!
        val chunkSize = System.getenv("CHUNK_SIZE")!!.toInt()
        val releaseCode = if (System.getenv("RELEASE_TYPE") == "INTERNATIONAL") 167
            else if (System.getenv("RELEASE_TYPE") == "BELGIQUE") 190440
            else 0

        val downloader = ReleaseDownloader(basePath)
        val release = downloader.getSnomedReleases(releaseCode) ?: throw IllegalStateException("No release list found")
        val latestRelease = release.getLatestRelease() ?: throw IllegalStateException("Cannot get latest release")
        val latestRF2 = latestRelease.getRF2() ?: throw IllegalStateException("Cannot get latest RF2")
        downloader.downloadRelease(snomedUserName, snomedPassword, latestRF2)
        if (!downloader.checkMD5(latestRelease)) throw IllegalStateException("Downloaded release MD5 does not match")
        val folders = downloader.getReleaseTypes(latestRF2)

        val folder = folders.delta ?: folders.snapshot ?: throw IllegalStateException("No valid release found")

        val conceptFile = File(folder).walk().filter { "sct2_Concept_[\\da-zA-Z_\\-]+.txt".toRegex().matches(it.name) }.firstOrNull() ?:
            throw IllegalStateException("Cannot find Concept File")

        val descriptionFiles = File(folder).walk().filter { "sct2_Description_[\\da-zA-Z_\\-]+.txt".toRegex().matches(it.name) }.toSet()
        if (descriptionFiles.isEmpty()) throw IllegalStateException("No description file found")

        val relationshipFile = File(folder).walk().filter { "sct2_Relationship_[\\da-zA-Z_\\-]+.txt".toRegex().matches(it.name) }.firstOrNull() ?:
        throw IllegalStateException("Cannot find Concept File")

        val region =
            Regex(pattern = ".*sct2_Concept_.+_([A-Z]{2,3}).*").find(conceptFile.name)?.groupValues?.get(1)
                ?.lowercase()
                ?: throw IllegalStateException("Cannot infer region code from filename!")

        val codes = retrieveCodesAndUpdates(region, conceptFile, descriptionFiles, relationshipFile)

        runBlocking {

            val codeApi = CodeApi(basePath = iCureUrl, authHeader = basicAuth(userName, password))

            batchDBUpdate(
                codes,
                codeType,
                chunkSize,
                codeApi,
                CommandlineProgressBar("Updating codes...", codes.size, 5)
            )
            println("")
        }
    }

}

@ExperimentalCoroutinesApi
@ExperimentalStdlibApi
fun main(args: Array<String>) {
    runApplication<ImporterApplication>(*args)
}
