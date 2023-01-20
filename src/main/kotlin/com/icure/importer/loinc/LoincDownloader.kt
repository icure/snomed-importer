package com.icure.importer.loinc

import com.icure.importer.controllers.CodificationParameters
import com.icure.importer.download.LoincReleaseDownloader
import com.icure.importer.utils.*
import io.icure.kraken.client.apis.CodeApi
import kotlinx.coroutines.*
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import java.io.File

@Component
class LoincDownloadLogic(
    val processCache: ProcessCache,
    val parser: MultiLanguageParser,
    @Value("\${importer.base-folder}") private val basePath: String,
    @Value("\${importer.chunk-size}") private val chunkSize: Int,
) {

    private val downloader = LoincReleaseDownloader("$basePath/loinc")
    private val defaultScope = CoroutineScope(Dispatchers.Default)

    private fun getLoincFQN(fields: List<String>, columns: Map<String, Int>) =
        "${fields[columns["COMPONENT"]!!]}:" +
                "${fields[columns["PROPERTY"]!!]}:" +
                "${fields[columns["TIME_ASPCT"]!!]}:" +
                "${fields[columns["SYSTEM"]!!]}:" +
                fields[columns["SCALE_TYP"]!!] +
                ((":" + fields[columns["METHOD_TYP"]!!]).takeIf { it != ":" } ?: "")

    @OptIn(ExperimentalStdlibApi::class, ExperimentalCoroutinesApi::class)
    suspend fun updateLoincCodes(
        processId: String,
        loincUsername: String,
        loincPassword: String,
        iCureUrl: String,
        iCureUsername: String,
        iCurePassword: String,
    ) = defaultScope.launch {

        try {
            processCache.getProcess(processId)?.let {
                processCache.updateProcess(
                    processId, it.copy(
                        started = System.currentTimeMillis(),
                        status = ProcessStatus.PARSING,
                        message = "Downloading and parsing codification files"
                    )
                )
            }

            downloader.downloadRelease(loincUsername, loincPassword)

            val newCodes = sortedMapOf<String, CodeUpdate>(compareBy { it.lowercase() })

            val tableColumns = File("$basePath/loinc/LoincTable/Loinc.csv").fieldColumnAssociation()

            File("$basePath/loinc/LoincTable/Loinc.csv").forEachLine { line ->
                if(!isActive) throw CancellationException("Operation was cancelled by the user")
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
                        fields[tableColumns["VersionLastChanged"]!!],
                        fields[tableColumns["STATUS"]!!] != "ACTIVE",
                        mutableMapOf("en" to getLoincFQN(fields, tableColumns)),
                        mutableMapOf("en" to names),
                        searchTerms = names.firstOrNull()?.let { mutableMapOf("en" to parser.getTokens("en", it)) }
                            ?: mutableMapOf()
                    )
                }
            }

            listOf(Pair("fr", "be"), Pair("fr", "fr"), Pair("nl", "nl")).forEach { languageRegion ->
                val localVariantFile = File("$basePath/loinc/AccessoryFiles/LinguisticVariants")
                    .walk()
                    .first { it.name.matches(Regex("${languageRegion.first}${languageRegion.second.uppercase()}[0-9]+LinguisticVariant\\.csv")) }

                val localColumns = localVariantFile.fieldColumnAssociation()
                localVariantFile.forEachLine { line ->
                    if(!isActive) throw CancellationException("Operation was cancelled by the user")
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
                                it.searchTerms[languageRegion.first] = parser.getTokens(languageRegion.first, sentence)
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
        } catch(e: Exception) {
            processCache.getProcess(processId)?.let {
                processCache.updateProcess(
                    processId,
                    it.copy(
                        status = ProcessStatus.STOPPED,
                        eta = null,
                        stacktrace = e.stackTraceToString(),
                        message = e.message ?: "Process interrupted due to an error"
                    )
                )
            }
        }
    }

}