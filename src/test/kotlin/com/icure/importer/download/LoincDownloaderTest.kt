package com.icure.importer.download

import io.kotest.core.spec.style.StringSpec
import java.io.File

class LoincDownloaderTest : StringSpec({

    val basePath = System.getenv("BASE_PATH")
    val username = System.getenv("LOINC_USERNAME")
    val password = System.getenv("LOINC_PASSWORD")

    "Can download the latest release of LOINC codes" {
        val downloader = LoincReleaseDownloader("$basePath/loinc")
        downloader.downloadRelease(username, password)
    }

})