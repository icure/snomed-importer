package com.icure.snomed.importer

import io.icure.kraken.client.apis.CodeApi
import io.icure.kraken.client.models.CodeDto
import io.icure.kraken.client.models.ListOfIdsDto
import io.icure.kraken.client.models.filter.code.CodeIdsByTypeCodeVersionIntervalFilter
import kotlinx.coroutines.ExperimentalCoroutinesApi
import java.io.File
import java.util.concurrent.TimeUnit

data class CodeBatches(
    val createBatch: List<CodeDto>,
    val updateBatch: List<CodeDto>
)

fun basicAuth(userName: String, password: String) =
    "Basic ${java.util.Base64.getEncoder().encodeToString("$userName:$password".toByteArray())}"

fun sanitize(text: String): String {
    return text.replace(",", "\\,")
}

fun makeCodeFromUpdate(update: SnomedCTCodeUpdate, type: String, oldCode: CodeDto? = null): CodeDto {
    val newLinks = oldCode?.qualifiedLinks?.toMutableMap() ?: mutableMapOf()
    update.relationsAdd.forEach { (k, v) ->
        newLinks[k] = ((newLinks[k]?.toSet() ?: setOf()) + v.toSet()).toList()
    }
    update.relationsRemove.forEach { (k, v) ->
        newLinks[k] = ((newLinks[k]?.toSet() ?: setOf()) - v.toSet()).toList()
    }

    update.synonyms.forEach { (k, v) ->
        if (update.description[k] == null) {
            update.description[k] = v[0]
        }
    }

    return oldCode?.copy(
            id = "SNOMED|${update.code}|${update.version ?: oldCode.version}",
            version = update.version ?: oldCode.version,
            label = oldCode.label?.plus(update.description) ?: update.description,
            regions = (oldCode.regions.toSet() + setOf(update.region)).toList(),
            qualifiedLinks = newLinks.filter { (_, v) -> v.isNotEmpty() },
            searchTerms = update.searchTerms.ifEmpty { oldCode.searchTerms },
            disabled = update.disabled ?: oldCode.disabled
        ) ?: CodeDto(
        id = "$type|${update.code}|${update.version}",
        type = "SNOMED",
        code = update.code,
        version = update.version,
        label = update.description,
        regions = listOf(update.region),
        qualifiedLinks = newLinks.filter { (_, v) -> v.isNotEmpty() },
        searchTerms = update.searchTerms,
        disabled = update.disabled ?: false
    )
}

// Adds the codes from a delta release
fun retrieveCodesAndUpdates(
    region: String,
    conceptFile: File,
    descriptionFiles: Set<File>,
    relationshipFile: File
): Map<String, SnomedCTCodeUpdate> {
    val parsers = mutableMapOf<String, SentenceParser>()    // Parsers in different languages to elaborate search terms
    val codes = sortedMapOf<String, SnomedCTCodeUpdate>(compareBy{ it.lowercase() })

    // First, I parse all the concepts
    val conceptsBar = CommandlineProgressBar("Parsing codes")
    conceptFile.forEachLine {
        conceptsBar.print()
        conceptsBar.step()
        val (conceptId, conceptVersion, active, _, _) = it.split("\t")
        if (conceptId != "id"){
            codes[conceptId] = SnomedCTCodeUpdate(conceptId, region, conceptVersion, active == "0")
        }
    }
    conceptsBar.print()
    println("")
    // Second, I parse all the descriptions, and I assign them to the concepts
    descriptionFiles.forEach {file ->
        val descBar = CommandlineProgressBar("Parsing descriptions from ${file.name}")
        file.forEachLine {
            descBar.print()
            descBar.step()
            val (_, _, active, _, conceptId, language, typeId, term, _) = it.split("\t")
            if (active == "1") {
                //Creates a new dummy code if it doesn't exist a code relative do this description
                if (codes[conceptId] == null) codes[conceptId] = SnomedCTCodeUpdate(conceptId, region)

                // Create search terms
                val searchTerms = parsers[language]?.getTokens(term)
                    ?: parsers.put(language, createSentenceParser(language))?.getTokens(term)
                    ?: setOf()

                codes[conceptId]!!.searchTerms[language] =
                    codes[conceptId]!!.searchTerms[language]?.plus(searchTerms) ?: searchTerms

                // Add fully qualified name
                if (typeId == "900000000000003001") codes[conceptId]!!.description[language] = sanitize(term)
                else codes[conceptId]!!.synonyms[language] =
                    codes[conceptId]!!.synonyms[language]?.plus(listOf(sanitize(term))) ?: listOf(sanitize(term))
            }
        }
        descBar.print()
        println("")
    }


    // Third, I parse the relations, and I assign them to the concepts
    val relBar = CommandlineProgressBar("Parsing relationship file")
    relationshipFile.forEachLine {
        relBar.print()
        relBar.step()
        val (_, _, active, _, sourceId, destinationId, _, typeId, _, _) = it.split("\t")
        if (sourceId != "sourceId") {
            //Creates a new dummy code if it doesn't exist a code relative do this description
            if (codes[sourceId] == null) codes[sourceId] = SnomedCTCodeUpdate(sourceId, region)

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
    relBar.print()
    println("")

    return codes
}

class CommandlineProgressBar(
    private val message: String,
    private val maxCount: Int? = null
) {
    private var count: Int = 0
    private var skipped: Int = 0
    private val deltas: Array<Long> = if (maxCount != null && maxCount > 50000) Array((maxCount*0.0002).toInt()) {0} else Array(10) {0}
    private var i: Int = 0
    private var lastCheck: Long? = null
    private var hr: Long = 0
    private var min: Long = 0
    private var sec: Long = 0

    fun print() {
        val avgMillis = deltas.sum() / 10
        maxCount?.let {
            if (i == 0) {
                val total = avgMillis * (it - count)
                hr = TimeUnit.MILLISECONDS.toHours(total)
                min = TimeUnit.MILLISECONDS.toMinutes(total) - TimeUnit.HOURS.toMinutes(hr)
                sec =
                    TimeUnit.MILLISECONDS.toSeconds(total) - TimeUnit.MINUTES.toSeconds(min) - TimeUnit.HOURS.toSeconds(hr)
            }
            val progress = 20 * count / it
            print("$message ($avgMillis ms/it) [${"=".repeat(progress)}${".".repeat(20-progress)}] $count/${it} - ETA: $hr:$min:$sec - $skipped skipped\r")
        } ?: print("$message ($avgMillis ms/it) - Processed: $count - $skipped skipped\r")
    }

    fun step() {
        val end = System.currentTimeMillis()
        lastCheck?.let {
            deltas[i] = end - it
            i = (i+1)%10
        }
        lastCheck = end
        count++
    }

    fun addSkip() {
        skipped++
    }
}

@OptIn(ExperimentalStdlibApi::class, ExperimentalCoroutinesApi::class)
suspend fun batchDBUpdate(codes: Map<String, SnomedCTCodeUpdate>, codeType: String, chunkSize: Int, codeApi: CodeApi, progressBar: CommandlineProgressBar) =
    codes.keys.chunked(chunkSize).forEach { chunkCodesId ->
        // First, I look for the existing codes in the database. If multiple CodeDTOs exist for the same code,
        // I only choose the one of the highest version
        val existingCodesIds = codeApi.matchCodesBy(
            CodeIdsByTypeCodeVersionIntervalFilter(
                startType = codeType,
                startCode = codes[chunkCodesId.first()]!!.code,
                startVersion = codes[chunkCodesId.first()]!!.version,
                endType = codeType,
                endCode = codes[chunkCodesId.last()]!!.code,
                endVersion = codes[chunkCodesId.last()]!!.version
            )
        ).groupBy {
            val (type, code, _) = it.split('|')
            Pair(type, code)
        }.map { codes -> codes.value.maxWithOrNull(compareBy{ it.lowercase() })!! }

        // I retrieve the codes related to the existing ids
        val existingCodes = codeApi.getCodes(ListOfIdsDto(existingCodesIds)).associateBy { it.code }

        // For each code in the chunk
        val newCodes = chunkCodesId.fold(CodeBatches(listOf(), listOf())) { acc, it ->
            // If the new code is already in the database
            existingCodes[it]?.let { existingCode ->
                // If the code does not have a version or has the same version of the existing one
                // Then I update the existing one
                if (codes[it]!!.version == null || existingCode.version == codes[it]!!.version) CodeBatches(acc.createBatch, acc.updateBatch + makeCodeFromUpdate(codes[it]!!, codeType, existingCode))
                // Otherwise, I create a new code
                else CodeBatches(acc.createBatch + makeCodeFromUpdate(codes[it]!!, codeType), acc.updateBatch)
            } ?: codes[it]?.version?.let{ codeUpdate ->
                // If the code does not exist and has a version, I create a new code
                CodeBatches(acc.createBatch + makeCodeFromUpdate(codes[codeUpdate]!!, codeType), acc.updateBatch)
                // If the code does not exist and does not have a version, we have a problem
            } ?: acc.also { progressBar.addSkip() }
        }

        codeApi.createCodes(newCodes.createBatch).forEach { _ ->
            progressBar.print()
            progressBar.step()
        }

        codeApi.modifyCodes(newCodes.updateBatch).forEach { _ ->
            progressBar.print()
            progressBar.step()
        }

    }

