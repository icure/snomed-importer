package com.icure.importer.loinc

import com.icure.importer.download.LoincReleaseDownloader
import com.icure.importer.exceptions.ImportCanceledException
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
    @Value("\${importer.base-folder}") private val basePath: String
) {

    private val downloader = LoincReleaseDownloader("$basePath/loinc")

    fun generateTask(
        processId: String,
        loincUsername: String,
        loincPassword: String,
        iCureUrl: String,
        iCureUsername: String,
        iCurePassword: String,
        chunkSize: Int
    ) = LoincDownloadTask(
        downloader,
        parser,
        processCache,
        processId,
        loincUsername,
        loincPassword,
        iCureUrl,
        iCureUsername,
        iCurePassword,
        chunkSize,
        basePath
    )

}