package com.icure.snomed.importer.loinc

import com.icure.snomed.importer.download.LoincReleaseDownloader
import com.icure.snomed.importer.nlp.createSentenceParser
import com.icure.snomed.importer.utils.*
import io.icure.kraken.client.apis.CodeApi
import java.io.File
import kotlin.random.Random

fun getLoincFQN(fields: List<String>, columns: Map<String, Int>) =
    "${fields[columns["COMPONENT"]!!]}:" +
            "${fields[columns["PROPERTY"]!!]}:" +
            "${fields[columns["TIME_ASPCT"]!!]}:" +
            "${fields[columns["SYSTEM"]!!]}:" +
            fields[columns["SCALE_TYP"]!!] +
            ((":"+fields[columns["METHOD_TYP"]!!]).takeIf { it != ":" } ?: "")

@OptIn(ExperimentalStdlibApi::class)
suspend fun updateLoincCodes(
    basePath: String,
    loincUsername: String,
    loincPassword: String,
    iCureUrl: String,
    iCureUsername: String,
    iCurePassword: String,
    chunkSize: Int
) {
    val downloader = LoincReleaseDownloader("$basePath/loinc")
    downloader.downloadRelease(loincUsername, loincPassword)

    val newCodes = sortedMapOf<String, CodeUpdate>(compareBy{ it.lowercase() })

    val tableColumns = File("$basePath/loinc/LoincTable/Loinc.csv").fieldColumnAssociation()

    val englishParser = createSentenceParser("en")
    val intBar = CommandlineProgressBar("Parsing International Codes")
    // File("$basePath/loinc/LoincTable/Loinc.csv").forEachLine { line ->
    File("$basePath/loinc/LoincTable/Loinc.csv").readLines().subList(0, 10000).forEach { line ->
        intBar.print()
        intBar.step()
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
                if (Random.nextBoolean()) "4" else fields[tableColumns["VersionLastChanged"]!!],
                fields[tableColumns["STATUS"]!!] != "ACTIVE",
                mutableMapOf("en" to getLoincFQN(fields, tableColumns)),
                mutableMapOf("en" to names),
                searchTerms = names.firstOrNull()?.let { mutableMapOf("en" to englishParser.getTokens(it)) } ?: mutableMapOf()
            )
        }
    }
    println("")

    listOf(Pair("fr", "be"), Pair("fr", "fr"), Pair("nl", "nl")).forEach { languageRegion ->
        val localVariantFile = File("$basePath/loinc/AccessoryFiles/LinguisticVariants")
            .walk()
            .first{ it.name.matches(Regex("${languageRegion.first}${languageRegion.second.uppercase()}[0-9]+LinguisticVariant\\.csv"))}

        val localColumns = localVariantFile.fieldColumnAssociation()
        val languageParser = createSentenceParser(languageRegion.first)
        val bar = CommandlineProgressBar("Parsing Local Variant: ${localVariantFile.name}")
        localVariantFile.forEachLine { line ->
            bar.print()
            bar.step()
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
                        it.searchTerms[languageRegion.first] = languageParser.getTokens(sentence)
                    }
                }
            }
        }
        println("")

    }

    val codeApi = CodeApi(basePath = iCureUrl, authHeader = basicAuth(iCureUsername, iCurePassword))

    batchDBUpdate(
        newCodes,
        "LOINC",
        chunkSize,
        codeApi,
        CommandlineProgressBar("Updating codes...", newCodes.size, 5)
    )
    println("")

}