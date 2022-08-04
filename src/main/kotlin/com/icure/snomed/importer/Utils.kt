package com.icure.snomed.importer

import io.icure.kraken.client.apis.CodeApi
import io.icure.kraken.client.models.CodeDto
import io.icure.kraken.client.models.ListOfIdsDto
import io.icure.kraken.client.models.filter.chain.FilterChain
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
            searchTerms = oldCode.searchTerms + update.searchTerms,
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

@OptIn(ExperimentalStdlibApi::class, ExperimentalCoroutinesApi::class)
suspend fun batchDBUpdate(codes: Map<String, SnomedCTCodeUpdate>, codeType: String, chunkSize: Int, codeApi: CodeApi, progressBar: CommandlineProgressBar) =
    codes.keys.chunked(chunkSize).fold(listOf<String>()) { generatedIds, chunkCodesId ->

        progressBar.print()
        // First, I look for the existing codes in the database. If multiple CodeDTOs exist for the same code,
        // I only choose the one of the highest version
        val existingCodes = codeApi.filterCodesBy(
            startKey = null,
            startDocumentId = null,
            limit = null,
            skip = null,
            sort = null,
            desc = null,
            filterChainCode = FilterChain(
                    CodeIdsByTypeCodeVersionIntervalFilter(
                    startType = codeType,
                    startCode = codes[chunkCodesId.first()]!!.code,
                    startVersion = codes[chunkCodesId.first()]!!.version,
                    endType = codeType,
                    endCode = codes[chunkCodesId.last()]!!.code,
                    endVersion = codes[chunkCodesId.last()]!!.version
                )
            )
        ).rows.groupBy {
            Pair(it.type!!, it.code!!)
        }.mapNotNull { groupedCodes ->
            //First, I check that the code is among the ones to be modified
            codes[groupedCodes.key.second]?.let { code ->
                // If the update has a version and matches with an existing one, then I want to update that code
                groupedCodes.value.firstOrNull { it.version == code.version }
                    ?:
                    if (code.version == null) groupedCodes.value.maxByOrNull { it.version!!.lowercase() } // If the update has no version, I want to update the latest code
                    else null // Else I want to create a new code from the update
            }
        }.associateBy { it.code }

        // For each code in the chunk
        val newCodes = chunkCodesId.fold(CodeBatches(listOf(), listOf())) { acc, it ->
            // If the code exists, I have to update it
            existingCodes[it]?.let { existingCode ->
                CodeBatches(acc.createBatch, acc.updateBatch + makeCodeFromUpdate(codes[it]!!, codeType, existingCode))
            } ?: codes[it]?.version?.let{ _ -> // Otherwise, I create a new code
                // If the code does not exist and has a version, I create a new code
                CodeBatches(acc.createBatch + makeCodeFromUpdate(codes[it]!!, codeType), acc.updateBatch)
                // If the code does not exist and does not have a version, we have a problem
            } ?: acc.also { progressBar.addSkip() }
        }

        val createdIds = codeApi.createCodes(newCodes.createBatch).map { it.id }

        val updatedIds = codeApi.modifyCodes(newCodes.updateBatch).map { it.id }
        progressBar.step(chunkSize)
        progressBar.print()
        generatedIds + updatedIds + createdIds
    }
