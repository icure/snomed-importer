package com.icure.snomed.importer

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.fasterxml.jackson.module.kotlin.SingletonSupport
import io.icure.kraken.client.apis.CodeApi
import io.icure.kraken.client.models.CodeDto
import io.icure.kraken.client.models.ListOfIdsDto
import kotlinx.collections.immutable.persistentHashSetOf
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.runBlocking
import org.springframework.boot.CommandLineRunner
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.core.io.support.PathMatchingResourcePatternResolver
import java.io.File
import java.nio.file.Files
import java.time.Instant
import java.util.IllegalFormatCodePointException
import kotlin.io.path.Path
import kotlin.io.path.forEachLine

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

    val objectMapper: ObjectMapper by lazy {
        ObjectMapper().registerModule(
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

    override fun run(args: Array<String>) {
        val basePath = System.getenv("BASE_PATH")!!
        val userName = System.getenv("ICURE_USER")!!
        val password = System.getenv("ICURE_PWD")!!
        val iCureUrl = System.getenv("ICURE_URL")!!
        val codeType = System.getenv("CODE_TYPE")!!
        val snomedUserName = System.getenv("SNOMED_USER")!!
        val snomedPassword = System.getenv("SNOMED_PWD")!!

        val downloader = ReleaseDownloader(basePath)
        val release = downloader.getSnomedReleases(167) ?: throw IllegalStateException("No release list found")
        val latestRelease = release.getLatestRelease() ?: throw IllegalStateException("Cannot get latest release")
        val latestRF2 = latestRelease.getRF2() ?: throw IllegalStateException("Cannot get latest RF2")
        downloader.downloadRelease(snomedUserName, snomedPassword, latestRF2)
        if (!downloader.checkMD5(latestRelease)) throw IllegalStateException("Downloaded release MD5 does not match")
        val folders = downloader.getReleaseTypes(latestRF2)

        val folder = folders.delta ?: folders.snapshot ?: throw IllegalStateException("No valid release found")

        val conceptFile = File(folder).walk().filter { "sct2_Concept_[0-9a-zA-Z_\\-]+.txt".toRegex().matches(it.name) }.firstOrNull() ?:
            throw IllegalStateException("Cannot find Concept File")

        val descriptionFiles = File(folder).walk().filter { "sct2_Description_[0-9a-zA-Z_\\-]+.txt".toRegex().matches(it.name) }.toSet()
        if (descriptionFiles.isEmpty()) throw IllegalStateException("No description file found")

        val relationshipFile = File(folder).walk().filter { "sct2_Relationship_[0-9a-zA-Z_\\-]+.txt".toRegex().matches(it.name) }.firstOrNull() ?:
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
                1000,
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
