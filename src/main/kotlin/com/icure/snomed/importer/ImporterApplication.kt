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
        val userName = args[1]
        val password = args[2]
        val iCureUrl = args[3]
        val codeType = args[4]

        val resolver = PathMatchingResourcePatternResolver(javaClass.classLoader)

        val conceptFile = resolver.getResources("classpath*:${args[0]}/sct2_Concept_**.txt")
            .firstOrNull()?.file ?: throw IllegalStateException("Concept file not found in ${args[0]}")
        val descriptionFiles = resolver.getResources("classpath*:${args[0]}/sct2_Description_**.txt")
            .fold(setOf<File>()) { map, it ->
                map + it.file
            }
        val relationshipFile = resolver.getResources("classpath*:${args[0]}/sct2_Relationship_**.txt")
            .firstOrNull()?.file ?: throw IllegalStateException("Relationship file not found in ${args[0]}")

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
