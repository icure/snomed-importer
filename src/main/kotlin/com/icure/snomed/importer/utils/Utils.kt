package com.icure.snomed.importer.utils

import io.icure.kraken.client.apis.CodeApi
import io.icure.kraken.client.models.CodeDto
import io.icure.kraken.client.models.filter.chain.FilterChain
import io.icure.kraken.client.models.filter.code.CodeIdsByTypeCodeVersionIntervalFilter
import kotlinx.coroutines.ExperimentalCoroutinesApi
import java.io.File

data class CodeUpdate(
    val code: String,
    val regions: MutableSet<String> = mutableSetOf(),
    val version: String? = null,
    val disabled: Boolean? = null,
    val description: MutableMap<String, String> = mutableMapOf(),
    val synonyms: MutableMap<String, List<String>> = mutableMapOf(),
    val relationsAdd: MutableMap<String, List<String>> = mutableMapOf(),
    val relationsRemove: MutableMap<String, List<String>> = mutableMapOf(),
    val searchTerms: MutableMap<String, Set<String>> = mutableMapOf()
)

data class CodeBatches(
    val createBatch: List<CodeDto>,
    val updateBatch: List<CodeDto>
)

operator fun List<String>.component3() = this[2]
operator fun List<String>.component4() = this[3]
operator fun List<String>.component5() = this[4]
operator fun List<String>.component6() = this[5]
operator fun List<String>.component7() = this[6]
operator fun List<String>.component8() = this[7]
operator fun List<String>.component9() = this[8]
operator fun List<String>.component10() = this[9]

fun basicAuth(userName: String, password: String) =
    "Basic ${java.util.Base64.getEncoder().encodeToString("$userName:$password".toByteArray())}"

fun sanitize(text: String): String {
    return text.replace(",", "\\,")
}

fun File.fieldColumnAssociation() =
    this.readLines()
        .first()
        .split(",")
        .map { it.removeSurrounding("\"") }
        .foldIndexed(emptyMap<String, Int>()) { id, acc, it ->
            acc + (it to id)
        }

@OptIn(ExperimentalStdlibApi::class, ExperimentalCoroutinesApi::class)
suspend fun CodeApi.filterCodesRecursive(codeType: String, startCode: String, startVersion: String?, endCode: String, endVersion: String?, limit: Int, accumulator: List<CodeDto> = listOf()): List<CodeDto> {
    val filterResult = this.filterCodesBy(
        startKey = null,
        startDocumentId = null,
        limit = limit,
        skip = null,
        sort = null,
        desc = null,
        filterChainCode = FilterChain(
            CodeIdsByTypeCodeVersionIntervalFilter(
                startType = codeType,
                startCode = startCode,
                startVersion = startVersion,
                endType = codeType,
                endCode = endCode,
                endVersion = endVersion
            )
        )
    )
    return if (filterResult.rows.isEmpty() || filterResult.rows.size < limit)
        accumulator + filterResult.rows
    else
        this.filterCodesRecursive(
            codeType,
            filterResult.rows.last().code!!,
            filterResult.rows.last().version,
            endCode,
            endVersion,
            limit,
            accumulator + filterResult.rows
        )
}

fun makeCodeFromUpdate(update: CodeUpdate, type: String, oldCode: CodeDto? = null): CodeDto {
    val newLinks = oldCode?.qualifiedLinks?.toMutableMap() ?: mutableMapOf()
    update.relationsAdd.forEach { (k, v) ->
        newLinks[k] = ((newLinks[k]?.toSet() ?: setOf()) + v.toSet()).toList()
    }
    update.relationsRemove.forEach { (k, v) ->
        newLinks[k] = ((newLinks[k]?.toSet() ?: setOf()) - v.toSet()).toList()
    }

    update.synonyms.forEach { (k, v) ->
        if (update.description[k].isNullOrBlank()) {
            update.description[k] = v[0]
        }
    }

    return oldCode?.copy(
        id = "$type|${update.code}|${update.version ?: oldCode.version}",
        version = update.version ?: oldCode.version,
        label = oldCode.label?.plus(update.description) ?: update.description,
        regions = (oldCode.regions.toSet() + update.regions.toSet()).toList(),
        qualifiedLinks = newLinks.filter { (_, v) -> v.isNotEmpty() },
        searchTerms = oldCode.searchTerms + update.searchTerms,
        disabled = update.disabled ?: oldCode.disabled
    ) ?: CodeDto(
        id = "$type|${update.code}|${update.version}",
        type = type,
        code = update.code,
        version = update.version,
        label = update.description,
        regions = update.regions.toList(),
        qualifiedLinks = newLinks.filter { (_, v) -> v.isNotEmpty() },
        searchTerms = update.searchTerms,
        disabled = update.disabled ?: false
    )
}

@OptIn(ExperimentalStdlibApi::class, ExperimentalCoroutinesApi::class)
suspend fun batchDBUpdate(codes: Map<String, CodeUpdate>, codeType: String, chunkSize: Int, codeApi: CodeApi, progressBar: CommandlineProgressBar) =
    codes.keys.chunked(chunkSize).fold(listOf<String>()) { generatedIds, chunkCodesId ->

        progressBar.print()
        // First, I look for the existing codes in the database. If multiple CodeDTOs exist for the same code,
        // I only choose the one of the highest version
        val existingCodes = codeApi.filterCodesRecursive(
            codeType = codeType,
            startCode = codes[chunkCodesId.first()]!!.code,
            startVersion = codes[chunkCodesId.first()]!!.version,
            endCode = codes[chunkCodesId.last()]!!.code,
            endVersion = codes[chunkCodesId.last()]!!.version,
            limit = chunkSize
        ).groupBy {
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
