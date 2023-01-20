package com.icure.importer.snomed

import com.icure.importer.download.SnomedReleaseDownloader
import com.icure.importer.exceptions.ImportCanceledException
import com.icure.importer.utils.*
import io.icure.kraken.client.apis.CodeApi
import kotlinx.coroutines.*
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import java.io.File

@Component
class SnomedDownloadLogic(
    val processCache: ProcessCache,
    val parser: MultiLanguageParser,
    @Value("\${importer.base-folder}") private val basePath: String
) {

    private val defaultScope = CoroutineScope(Dispatchers.Default)

    @OptIn(ExperimentalStdlibApi::class, ExperimentalCoroutinesApi::class)
    suspend fun updateSnomedCodes(
        processId: String,
        releaseCode: Int,
        releaseType: String,
        snomedUsername: String,
        snomedPassword: String,
        iCureUrl: String,
        iCureUsername: String,
        iCurePassword: String,
        chunkSize: Int
    ) = defaultScope.launch(setStatusErrorHandler(processId, processCache)) {

        processCache.getProcess(processId)?.let {
            processCache.updateProcess(processId, it.copy(
                started = System.currentTimeMillis(),
                status = ProcessStatus.PARSING,
                message = "Downloading and parsing codification files"
            ))
        }

        val downloader = SnomedReleaseDownloader(basePath)
        val release = downloader.getSnomedReleases(releaseCode)
        val latestRelease = release.getLatestRelease() ?: throw IllegalStateException("Cannot get latest release")
        val latestRF2 = latestRelease.getRF2() ?: throw IllegalStateException("Cannot get latest RF2")
        downloader.downloadRelease(snomedUsername, snomedPassword, latestRF2)
        if (!downloader.checkMD5(latestRelease)) throw IllegalStateException("Downloaded release MD5 does not match")
        val folders = downloader.getReleaseTypes(latestRF2)

        val folder = when(releaseType) {
            "snapshot" -> folders.snapshot
            "delta" -> folders.delta
            else -> null
        } ?: throw IllegalStateException("No valid release found")

        val conceptFile =
            File(folder).walk().filter { "sct2_Concept_[\\da-zA-Z_\\-]+.txt".toRegex().matches(it.name) }.firstOrNull()
                ?: throw IllegalStateException("Cannot find Concept File")

        val descriptionFiles =
            File(folder).walk().filter { "sct2_Description_[\\da-zA-Z_\\-]+.txt".toRegex().matches(it.name) }.toSet()
        if (descriptionFiles.isEmpty()) throw IllegalStateException("No description file found")

        val relationshipFile =
            File(folder).walk().filter { "sct2_Relationship_[\\da-zA-Z_\\-]+.txt".toRegex().matches(it.name) }
                .firstOrNull() ?: throw IllegalStateException("Cannot find Concept File")

        val region =
            Regex(pattern = ".*sct2_Concept_.+_([A-Z]{2,3}).*").find(conceptFile.name)?.groupValues?.get(1)
                ?.lowercase()
                ?: throw IllegalStateException("Cannot infer region code from filename!")

        val codes = retrieveCodesAndUpdates(
            if (region == "int") "xx" else region,
            conceptFile,
            descriptionFiles,
            relationshipFile
        )

        val codeApi = CodeApi(basePath = iCureUrl, authHeader = basicAuth(iCureUsername, iCurePassword))

        batchDBUpdate(
            codes,
            "SNOMED",
            chunkSize,
            codeApi,
            processCache,
            processId
        )
    }

    private suspend fun retrieveCodesAndUpdates(
        region: String,
        conceptFile: File,
        descriptionFiles: Set<File>,
        relationshipFile: File
    ): Map<String, CodeUpdate> = withContext(Dispatchers.Default) {
        val codes = sortedMapOf<String, CodeUpdate>(compareBy{ it.lowercase() })

        // First, I parse all the concepts
        conceptFile.forEachLine {
            if(!isActive) throw ImportCanceledException()
            val (conceptId, conceptVersion, active, _, _) = it.split("\t")
            if (conceptId != "id"){
                codes[conceptId] = CodeUpdate(conceptId, mutableSetOf(region), conceptVersion, active == "0")
            }
        }

        // Second, I parse all the descriptions, and I assign them to the concepts
        descriptionFiles.forEach {file ->
            file.forEachLine {
                if(!isActive) throw ImportCanceledException()
                val (_, _, active, _, conceptId, language, typeId, term, _) = it.split("\t")
                if (active == "1") {
                    //Creates a new dummy code if it doesn't exist a code relative do this description
                    if (codes[conceptId] == null) codes[conceptId] = CodeUpdate(conceptId, mutableSetOf(region))

                    // Create search terms
                    val searchTerms = parser.getTokens(language, term)

                    codes[conceptId]!!.searchTerms[language] =
                        codes[conceptId]!!.searchTerms[language]?.plus(searchTerms) ?: searchTerms

                    // Add fully qualified name
                    if (typeId == "900000000000003001") codes[conceptId]!!.description[language] = sanitize(term)
                    else codes[conceptId]!!.synonyms[language] =
                        codes[conceptId]!!.synonyms[language]?.plus(listOf(sanitize(term))) ?: listOf(sanitize(term))
                }
            }
        }


        // Third, I parse the relations, and I assign them to the concepts
        relationshipFile.forEachLine {
            if(!isActive) throw ImportCanceledException()
            val (_, _, active, _, sourceId, destinationId, _, typeId, _, _) = it.split("\t")
            if (sourceId != "sourceId") {
                //Creates a new dummy code if it doesn't exist a code relative do this description
                if (codes[sourceId] == null) codes[sourceId] = CodeUpdate(sourceId, mutableSetOf(region))

                // If the relation is active, then it must be added
                if (active == "1") {
                    codes[sourceId]!!.relationsAdd[typeId] =
                        codes[sourceId]!!.relationsAdd[typeId]?.plus(destinationId)
                            ?: listOf(destinationId)
                } else { // Otherwise it must be removed
                    codes[sourceId]!!.relationsRemove[typeId] =
                        codes[sourceId]!!.relationsRemove[typeId]?.plus(destinationId)
                            ?: listOf(destinationId)
                }
            }
        }

        codes
    }
}