package com.icure.snomed.importer.snomed

import com.icure.snomed.importer.download.SnomedReleaseDownloader
import com.icure.snomed.importer.utils.CommandlineProgressBar
import com.icure.snomed.importer.utils.basicAuth
import com.icure.snomed.importer.utils.batchDBUpdate
import com.icure.snomed.importer.utils.retrieveCodesAndUpdates
import io.icure.kraken.client.apis.CodeApi
import kotlinx.coroutines.ExperimentalCoroutinesApi
import java.io.File

@OptIn(ExperimentalStdlibApi::class, ExperimentalCoroutinesApi::class)
suspend fun updateSnomedCodes(
    basePath: String,
    releaseCode: Int,
    snomedUserName: String,
    snomedPassword: String,
    userName: String,
    password: String,
    iCureUrl: String,
    chunkSize: Int
) {
    val downloader = SnomedReleaseDownloader(basePath)
    val release = downloader.getSnomedReleases(releaseCode)
    val latestRelease = release.getLatestRelease() ?: throw IllegalStateException("Cannot get latest release")
    val latestRF2 = latestRelease.getRF2() ?: throw IllegalStateException("Cannot get latest RF2")
    downloader.downloadRelease(snomedUserName, snomedPassword, latestRF2)
    if (!downloader.checkMD5(latestRelease)) throw IllegalStateException("Downloaded release MD5 does not match")
    val folders = downloader.getReleaseTypes(latestRF2)

    val folder = folders.delta ?: folders.snapshot ?: throw IllegalStateException("No valid release found")

    val conceptFile = File(folder).walk().filter { "sct2_Concept_[\\da-zA-Z_\\-]+.txt".toRegex().matches(it.name) }.firstOrNull() ?:
    throw IllegalStateException("Cannot find Concept File")

    val descriptionFiles = File(folder).walk().filter { "sct2_Description_[\\da-zA-Z_\\-]+.txt".toRegex().matches(it.name) }.toSet()
    if (descriptionFiles.isEmpty()) throw IllegalStateException("No description file found")

    val relationshipFile = File(folder).walk().filter { "sct2_Relationship_[\\da-zA-Z_\\-]+.txt".toRegex().matches(it.name) }.firstOrNull() ?:
    throw IllegalStateException("Cannot find Concept File")

    val region =
        Regex(pattern = ".*sct2_Concept_.+_([A-Z]{2,3}).*").find(conceptFile.name)?.groupValues?.get(1)
            ?.lowercase()
            ?: throw IllegalStateException("Cannot infer region code from filename!")

    val codes = retrieveCodesAndUpdates(
        if (region == "int") "xx" else region,
        conceptFile,
        descriptionFiles,
        relationshipFile)

    val codeApi = CodeApi(basePath = iCureUrl, authHeader = basicAuth(userName, password))

    batchDBUpdate(
        codes,
        "SNOMED",
        chunkSize,
        codeApi,
        CommandlineProgressBar("Updating codes...", codes.size, 5)
    )
    println("")
}