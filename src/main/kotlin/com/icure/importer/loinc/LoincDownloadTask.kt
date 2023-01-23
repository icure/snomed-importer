package com.icure.importer.loinc

import com.icure.importer.download.LoincReleaseDownloader
import com.icure.importer.exceptions.ImportCanceledException
import com.icure.importer.scheduler.DownloadTask
import com.icure.importer.utils.*
import io.icure.kraken.client.apis.CodeApi
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.withContext
import java.io.File

class LoincDownloadTask(
    private val downloader: LoincReleaseDownloader,
    private val parser: MultiLanguageParser,
    private val processCache: ProcessCache,
    override val processId: String,
    private val loincUsername: String,
    private val loincPassword: String,
    private val iCureUrl: String,
    private val iCureUsername: String,
    private val iCurePassword: String,
    private val chunkSize: Int,
    private val basePath: String
) : DownloadTask {

    init {
        processCache.updateProcess(
            processId,
            Process(
                processId,
                ProcessStatus.QUEUED,
                System.currentTimeMillis(),
                message = "Waiting to start the process"
            )
        )
    }

    @OptIn(ExperimentalStdlibApi::class, ExperimentalCoroutinesApi::class)
    override suspend fun execute(): Unit = withContext(Dispatchers.Default) {
        processCache.getProcess(processId)?.let {
            processCache.updateProcess(
                processId, it.copy(
                    started = System.currentTimeMillis(),
                    status = ProcessStatus.DOWNLOADING,
                    message = "Downloading codification files"
                )
            )
        }

        downloader.downloadRelease(loincUsername, loincPassword)

        processCache.getProcess(processId)?.let {
            processCache.updateProcess(
                processId, it.copy(
                    status = ProcessStatus.PARSING,
                    message = "Parsing codification files"
                )
            )
        }

        val newCodes = sortedMapOf<String, CodeUpdate>(compareBy { it.lowercase() })

        val tableColumns = File("$basePath/loinc/LoincTable/Loinc.csv").fieldColumnAssociation()

        File("$basePath/loinc/LoincTable/Loinc.csv").forEachLine { line ->
            if(processCache.getProcess(processId)?.isCanceled() != false) throw ImportCanceledException()
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
                if(processCache.getProcess(processId)?.isCanceled() != false) throw ImportCanceledException()
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
    }

}