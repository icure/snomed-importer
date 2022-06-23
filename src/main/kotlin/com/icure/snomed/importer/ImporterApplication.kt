package com.icure.snomed.importer

import io.icure.kraken.client.apis.CodeApi
import io.icure.kraken.client.models.CodeDto
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.runBlocking
import org.springframework.boot.CommandLineRunner
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
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

data class SnomedCTCode(
    val code: String,
    val version: String,
    var description: String? = null,
    var language: String? = null,
    val synonyms: MutableList<String> = mutableListOf(),
    val relations: MutableMap<String, List<String>> = mutableMapOf(),
    val keywords: MutableSet<String> = mutableSetOf()
) {
    fun toCodeDTO(): CodeDto {
        return CodeDto(
            id = "SNOMED|$code|$version",
            type = "SNOMED",
            code = code,
            version = version,
            label = mapOf(language!! to description!!),
            qualifiedLinks = relations,
            searchTerms = mapOf(language!! to keywords)
        )
    }
}

@SpringBootApplication
@ExperimentalCoroutinesApi
@ExperimentalStdlibApi
class ImporterApplication : CommandLineRunner {
    // Given the files of a FULL SNOMED CT release, it finds all the versions it contains
    fun getVersions(conceptFile: String, descriptionFile: String, relationshipFile: String): Set<String> {
        val versionSet = HashSet<String>()
        Path(conceptFile).forEachLine(Charsets.UTF_8) {
            val (_, version, _, _, _) = it.split("\t")
            versionSet.add(version)
        }
        Path(descriptionFile).forEachLine(Charsets.UTF_8) {
            val (_, version, _, _, _, _, _, _, _) = it.split("\t")
            versionSet.add(version)
        }
        Path(relationshipFile).forEachLine(Charsets.UTF_8) {
            val (_, version, _, _, _, _, _, _, _, _) = it.split("\t")
            versionSet.add(version)
        }
        return versionSet
    }

    // Adds the codes from a delta release
    fun parseDelta(
        version: String,
        conceptFile: String,
        descriptionFile: String,
        relationshipFile: String
    ): Collection<SnomedCTCode> {
        val codes = mutableMapOf<String, SnomedCTCode>() // Data structure that contains all the codes for that release
        val parser = CoreNLPParser(setOf("FW", "JJ", "NN", "NNS", "NNP", "NNPS"))    // Parser to elaborate search terms

        // First, I parse all the concepts
        Path(conceptFile).forEachLine(Charsets.UTF_8) {
            val (conceptId, conceptVersion, active, _, _) = it.split("\t")
            if (conceptVersion == version && active == "1") {
                codes[conceptId] = SnomedCTCode(conceptId, conceptVersion)
            }
        }

        // Second, I parse all the descriptions, and I assign them to the concepts
        Path(descriptionFile).forEachLine(Charsets.UTF_8) {
            val (conceptId, descriptionVersion, active, _, _, language, typeId, term, _) = it.split("\t")
            if (descriptionVersion == version && active == "1") {
                codes[conceptId]!!.language = language                                      // If I find a description
                if (typeId == "900000000000003001") codes[conceptId]!!.description = term   // without code, it's a
            } else codes[conceptId]!!.synonyms.add(term)                                // problem
        }

        // Third, I parse the relations, and I assign them to the concepts
        Path(relationshipFile).forEachLine(Charsets.UTF_8) {
            val (_, relationVersion, active, _, sourceId, destinationId, _, typeId, _, _) = it.split("\t")
            if (relationVersion == version && active == "1") {
                codes[sourceId]!!.relations[typeId] =                           // If I find a relation without a code
                    codes[sourceId]!!.relations[typeId]?.plus(destinationId)    // it's also a problem
                        ?: listOf(destinationId)
            }
        }

        // Generates a set of keyword for each concept based on its fully qualified name, description, and fully
        // qualified name of the related concepts
        codes.forEach { (_, v) ->
            v.description?.let {
                v.keywords.addAll(parser.getTokensByPOSTags(it))
            }
            v.synonyms.forEach {
                v.keywords.addAll(parser.getTokensByPOSTags(it))
            }
            v.relations.forEach { (_, ids) ->
                ids.forEach { id ->
                    codes[id]?.description?.let {
                        v.keywords.addAll(parser.getTokensByPOSTags(it))
                    }
                }
            }
        }
        return codes.values
    }

    override fun run(args: Array<String>) {
        val userName = args[3]
        val password = args[4]

        runBlocking {
            val codeApi =
                CodeApi(basePath = "http://127.0.0.1:16043", authHeader = basicAuth(userName, password))

            getVersions(args[0], args[1], args[2]).forEach { ver ->
                parseDelta(ver, args[0], args[1], args[2]).forEach { code ->
                    codeApi.createCode(code.toCodeDTO())
                }
            }
        }
    }

    @ExperimentalCoroutinesApi
    @ExperimentalStdlibApi
    fun main(args: Array<String>) {
        runApplication<ImporterApplication>(*args)
    }
}
