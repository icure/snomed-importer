package com.icure.snomed.importer

import com.icure.snomed.importer.download.SnomedReleaseDownloader
import com.icure.snomed.importer.snomed.updateSnomedCodes
import com.icure.snomed.importer.utils.CommandlineProgressBar
import com.icure.snomed.importer.utils.basicAuth
import com.icure.snomed.importer.utils.batchDBUpdate
import com.icure.snomed.importer.utils.retrieveCodesAndUpdates
import io.icure.kraken.client.apis.CodeApi
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.runBlocking
import org.springframework.boot.CommandLineRunner
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import java.io.File

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

    override fun run(args: Array<String>) {
        val basePath = System.getenv("BASE_PATH")
        val userName = System.getenv("ICURE_USER")
        val password = System.getenv("ICURE_PWD")
        val iCureUrl = System.getenv("ICURE_URL")
        val snomedUserName = System.getenv("SNOMED_USER")
        val snomedPassword = System.getenv("SNOMED_PWD")
        val chunkSize = System.getenv("CHUNK_SIZE").toInt()
        val releaseCode = if (System.getenv("RELEASE_TYPE") == "INTERNATIONAL") 167
            else if (System.getenv("RELEASE_TYPE") == "BELGIUM") 190440
            else 0

        runBlocking {
            updateSnomedCodes(
                basePath,
                releaseCode,
                snomedUserName,
                snomedPassword,
                userName,
                password,
                iCureUrl,
                chunkSize
            )
        }
    }

}

@ExperimentalCoroutinesApi
@ExperimentalStdlibApi
fun main(args: Array<String>) {
    runApplication<ImporterApplication>(*args)
}
