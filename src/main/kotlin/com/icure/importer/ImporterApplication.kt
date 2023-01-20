package com.icure.importer


import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.runBlocking
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.PropertySource

@SpringBootApplication(scanBasePackages = [
    "com.icure.importer.controllers",
    "com.icure.importer.loinc",
    "com.icure.importer.utils"
])
@PropertySource("classpath:application.properties")
@ExperimentalCoroutinesApi
@ExperimentalStdlibApi
class ImporterApplication {

    fun boh(args: Array<String>) {
        val basePath = System.getenv("BASE_PATH")
        val iCureUsername = System.getenv("ICURE_USER")
        val iCurePassword = System.getenv("ICURE_PWD")
        val iCureUrl = System.getenv("ICURE_URL")
        val snomedUserName = System.getenv("SNOMED_USER")
        val snomedPassword = System.getenv("SNOMED_PWD")
        val loincUsername = System.getenv("LOINC_USER")
        val loincPassword = System.getenv("LOINC_PWD")
        val chunkSize = 1000
        val releaseCode = if (System.getenv("RELEASE_TYPE") == "INTERNATIONAL") 167
            else if (System.getenv("RELEASE_TYPE") == "BELGIUM") 190440
            else 0

        runBlocking {
//            updateSnomedCodes(
//                basePath,
//                releaseCode,
//                snomedUserName,
//                snomedPassword,
//                userName,
//                password,
//                iCureUrl,
//                chunkSize
//            )
//            updateLoincCodes(
//                basePath,
//                loincUsername,
//                loincPassword,
//                iCureUrl,
//                iCureUsername,
//                iCurePassword,
//                chunkSize
//            )
        }
    }

}

@ExperimentalCoroutinesApi
@ExperimentalStdlibApi
fun main(args: Array<String>) {
    runApplication<ImporterApplication>(*args)
}
