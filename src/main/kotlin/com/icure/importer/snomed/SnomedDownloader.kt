package com.icure.importer.snomed

import com.icure.importer.utils.*
import kotlinx.coroutines.*
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component

@Component
class SnomedDownloadLogic(
    val processCache: ProcessCache,
    val parser: MultiLanguageParser,
    @Value("\${importer.base-folder}") private val basePath: String
) {

    fun generateTask(
        processId: String,
        regionCode: Int,
        releaseType: String,
        snomedUsername: String,
        snomedPassword: String,
        iCureUrl: String,
        iCureUsername: String,
        iCurePassword: String,
        chunkSize: Int
    ) = SnomedDownloadTask(
        parser,
        processCache,
        processId,
        snomedUsername,
        snomedPassword,
        iCureUrl,
        iCureUsername,
        iCurePassword,
        chunkSize,
        basePath,
        regionCode,
        releaseType
    )

}