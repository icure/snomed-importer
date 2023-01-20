package com.icure.importer.loinc

import com.icure.importer.download.LoincReleaseDownloader
import com.icure.importer.nlp.createSentenceParser
import com.icure.importer.utils.*
import io.icure.kraken.client.apis.CodeApi
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import java.io.File
import kotlin.random.Random

@Component
class LoincDownloadLogic(
    val processCache: ProcessCache,
    @Value("\${importer.base-folder}") private val basePath: String,
    @Value("\${importer.chunk-size}") private val chunkSize: Int,
) {

    private val downloader = LoincReleaseDownloader("$basePath/loinc")

    private fun getLoincFQN(fields: List<String>, columns: Map<String, Int>) =
        "${fields[columns["COMPONENT"]!!]}:" +
                "${fields[columns["PROPERTY"]!!]}:" +
                "${fields[columns["TIME_ASPCT"]!!]}:" +
                "${fields[columns["SYSTEM"]!!]}:" +
                fields[columns["SCALE_TYP"]!!] +
                ((":" + fields[columns["METHOD_TYP"]!!]).takeIf { it != ":" } ?: "")

    @OptIn(ExperimentalStdlibApi::class)
    suspend fun updateLoincCodes(
        processId: String,
        loincUsername: String,
        loincPassword: String,
        iCureUrl: String,
        iCureUsername: String,
        iCurePassword: String,
    ) {

        processCache.getProcess(processId)?.let {
            processCache.updateProcess(processId, it.copy(
                started = System.currentTimeMillis(),
                status = ProcessStatus.STARTED
            ))
        }

        downloader.downloadRelease(loincUsername, loincPassword)

        val newCodes = sortedMapOf<String, CodeUpdate>(compareBy { it.lowercase() })

        val tableColumns = File("$basePath/loinc/LoincTable/Loinc.csv").fieldColumnAssociation()

        val englishParser = createSentenceParser("en")
        File("$basePath/loinc/LoincTable/Loinc.csv").forEachLine { line ->
            val fields = line.removeSurrounding("\"").split("\",\"")
            if (fields[0] != "LOINC_NUM") {
                val names = listOf(
                    sanitize(fields[tableColumns["CONSUMER_NAME"]!!]),
                    sanitize(fields[tableColumns["LONG_COMMON_NAME"]!!]),
                    sanitize(fields[tableColumns["DisplayName"]!!]),
                    sanitize(fields[tableColumns["SHORTNAME"]!!]),
                    getLoincFQN(fields, tableColumns)
                ).filter { it.isNotBlank() }
                newCodes[fields[0]] = CodeUpdate(
                    fields[tableColumns["LOINC_NUM"]!!],
                    mutableSetOf("xx"),
                    if (Random.nextBoolean()) "4" else fields[tableColumns["VersionLastChanged"]!!],
                    fields[tableColumns["STATUS"]!!] != "ACTIVE",
                    mutableMapOf("en" to getLoincFQN(fields, tableColumns)),
                    mutableMapOf("en" to names),
                    searchTerms = names.firstOrNull()?.let { mutableMapOf("en" to englishParser.getTokens(it)) }
                        ?: mutableMapOf()
                )
            }
        }

        listOf(Pair("fr", "be"), Pair("fr", "fr"), Pair("nl", "nl")).forEach { languageRegion ->
            val localVariantFile = File("$basePath/loinc/AccessoryFiles/LinguisticVariants")
                .walk()
                .first { it.name.matches(Regex("${languageRegion.first}${languageRegion.second.uppercase()}[0-9]+LinguisticVariant\\.csv")) }

            val localColumns = localVariantFile.fieldColumnAssociation()
            val languageParser = createSentenceParser(languageRegion.first)
            localVariantFile.forEachLine { line ->
                val fields = line.removeSurrounding("\"").split("\",\"")
                if (fields[0] != "LOINC_NUM") {
                    val names = listOf(
                        sanitize(fields[localColumns["LONG_COMMON_NAME"]!!]),
                        sanitize(fields[localColumns["LinguisticVariantDisplayName"]!!]),
                        sanitize(fields[localColumns["SHORTNAME"]!!]),
                        getLoincFQN(fields, localColumns)
                    ).filter { name -> name.isNotBlank() }
                    newCodes[fields[0]]?.let {
                        it.regions.add(languageRegion.second)
                        it.description[languageRegion.first] = getLoincFQN(fields, localColumns)
                        names.firstOrNull()?.let { sentence ->
                            it.synonyms[languageRegion.first] = names
                            it.searchTerms[languageRegion.first] = languageParser.getTokens(sentence)
                        }
                    }
                }
            }

        }

        val codeApi = CodeApi(basePath = iCureUrl, authHeader = basicAuth(iCureUsername, iCurePassword))

        batchDBUpdate(
            newCodes,
            "LOINC",
            chunkSize,
            codeApi,
            processCache,
            processId
        )

    }

}