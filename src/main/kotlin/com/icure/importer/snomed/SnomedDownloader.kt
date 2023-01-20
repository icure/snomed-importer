package com.icure.importer.snomed

import com.icure.importer.download.SnomedReleaseDownloader
import com.icure.importer.utils.*
import io.icure.kraken.client.apis.CodeApi
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.withContext
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import java.io.File

@Component
class SnomedDownloadLogic(
    val processCache: ProcessCache,
    @Value("\${importer.base-folder}") private val basePath: String,
    @Value("\${importer.chunk-size}") private val chunkSize: Int,
) {

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
    ) {

        processCache.getProcess(processId)?.let {
            processCache.updateProcess(processId, it.copy(
                started = System.currentTimeMillis(),
                status = ProcessStatus.PARSING
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

        processCache.getProcess(processId)?.let {
            processCache.updateProcess(processId, it.copy(
                status = ProcessStatus.UPLOADING,
                uploadStarted = System.currentTimeMillis(),
                total = codes.size
            ))
        }

        withContext(Dispatchers.IO) {
            batchDBUpdate(
                codes,
                "SNOMED",
                chunkSize,
                codeApi,
                processCache,
                processId
            )
        }

    }
}