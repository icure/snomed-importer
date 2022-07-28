package com.icure.snomed.importer

import TestFileGenerator
import io.icure.kraken.client.apis.CodeApi
import io.icure.kraken.client.models.CodeDto
import io.icure.kraken.client.models.filter.chain.FilterChain
import io.icure.kraken.client.models.filter.code.AllCodesFilter
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import kotlinx.coroutines.ExperimentalCoroutinesApi
import java.io.File

fun CodeDto.containsUpdate(other: SnomedCTCodeUpdate): Boolean {
    return this.code == other.code &&
            this.regions.contains(other.region) &&
            this.disabled == other.disabled &&
            other.description.all { this.label != null && this.label!![it.key] == it.value } &&
            other.relationsRemove.all {
                this.qualifiedLinks[it.key] == null ||
                        (this.qualifiedLinks[it.key] != null && this.qualifiedLinks[it.key]!!.all { existingLink ->
                            !it.value.contains(existingLink)
                        })
            } &&
            other.relationsAdd.all {
                this.qualifiedLinks[it.key] != null &&
                        this.qualifiedLinks[it.key]!!.all { existingLink ->
                            it.value.contains(existingLink)
                        }
            } &&
            other.searchTerms.all {
                this.searchTerms[it.key] != null &&
                        this.searchTerms[it.key]!!.all { searchTerm ->
                            it.value.contains(searchTerm)
                        }
            }

}

@OptIn(ExperimentalStdlibApi::class, ExperimentalCoroutinesApi::class)
class ImporterApplicationTest : StringSpec({

    val conceptFilename = "build/tmp/test/test_Concept_E2E_Snapshot_INT_20210731.txt"
    val conceptDeltaFilename = "build/tmp/test/test_Concept_E2E_Delta_INT_20210731.txt"
    val descriptionFilename = "build/tmp/test/test_Description_E2E_Snapshot-en_INT_20210731.txt"
    val descriptionDeltaFilename = "build/tmp/test/test_Description_E2E_Delta-en_INT_20210731.txt"
    val relationshipFilename = "build/tmp/test/test_Relationship_E2E_Snapshot_INT_20210731"
    val relationshipDeltaFilename = "build/tmp/test/test_Relationship_E2e_Delta_INT_20210731"
    val numSamples = 200
    val codeApi = CodeApi(basePath = "http://127.0.0.1:16043", authHeader = basicAuth(System.getenv("USERNAME"), System.getenv("PASSWORD")))
    var insertedCodes: Map<String, CodeDto> = mapOf()


    // Creates some simulate Snapshot and Delta files and initializes a database with some codes
    beforeSpec {
        val tfg = TestFileGenerator()
        File(conceptFilename).printWriter().use {
            it.print(tfg.generateConcepts(numSamples, false))
        }
        File(conceptDeltaFilename).printWriter().use {
            it.print(tfg.generateConcepts(numSamples, true) +
                        tfg.generateConcepts(numSamples, true, numSamples/2))
        }
        File(descriptionFilename).printWriter().use {
            it.print(tfg.generateDescriptions(numSamples, false))
        }
        File(descriptionDeltaFilename).printWriter().use {
            it.print(tfg.generateDescriptions(numSamples, true)
                        + tfg.generateDescriptions(numSamples, true, numSamples/2))
        }
        File(relationshipFilename).printWriter().use {
            it.print(tfg.generateRelationships(numSamples, false))
        }
        File(relationshipDeltaFilename).printWriter().use {
            it.print(tfg.generateRelationships(numSamples, true)
                        + tfg.generateRelationships(numSamples, true, numSamples/2))
        }

        val conceptFile = File(conceptFilename)
        val descriptionFiles = setOf(File(descriptionFilename))
        val relationshipFile = File(relationshipFilename)

        val codes = retrieveCodesAndUpdates("int", conceptFile, descriptionFiles, relationshipFile)

        batchDBUpdate(
            codes,
            "SNOMED",
            100,
            codeApi,
            CommandlineProgressBar("Initializing codes...", codes.size)
        )
        println("")

        val insertionResult = codeApi.filterCodesBy(null, null, null, null, null, null, FilterChain(AllCodesFilter()))
        insertionResult.rows.size shouldBe numSamples
        insertedCodes = insertionResult.rows.associateBy { it.code!! }
    }

    "Adding and updating codes in the DB" {
        val conceptFile = File(conceptDeltaFilename)
        val descriptionFiles = setOf(File(descriptionDeltaFilename))
        val relationshipFile = File(relationshipDeltaFilename)

        val codes = retrieveCodesAndUpdates("int", conceptFile, descriptionFiles, relationshipFile)

        batchDBUpdate(
            codes,
            "SNOMED",
            100,
            codeApi,
            CommandlineProgressBar("Updating codes...", codes.size)
        )
        println("")

        val updateResult = codeApi.filterCodesBy(null, null, null, null, null, null, FilterChain(AllCodesFilter()))
        val updatedCodes = updateResult.rows.associateBy { it.code!! }

        codes.forEach { (codeId, code) ->
            // If the code does not exist in the DB but is a dummy, then it is not added
            if (insertedCodes[codeId] == null && code.version == null) {
                updatedCodes[codeId] shouldBe null
            }
            // If the code does not exist in the DB and is valid, it is added
            else if (insertedCodes[codeId] == null && code.version != null) {
                updatedCodes[codeId] shouldNotBe null
                updatedCodes[codeId]!!.containsUpdate(code) shouldBe true
                updatedCodes[codeId]!!.version shouldBe code.version
            }
            // If the code exists but the update doesn't specify a new version,
            // the code is updated and keeps the same version
            else if (insertedCodes[codeId] != null && (code.version == null || code.version == insertedCodes[codeId]!!.version)) {
                updatedCodes[codeId] shouldNotBe null
                updatedCodes[codeId]!!.containsUpdate(code) shouldBe true
                updatedCodes[codeId]!!.version shouldBe insertedCodes[codeId]!!.version
            }
            // If the code exists and the update specify a new version,
            // the code is updated with a new version
            else if (insertedCodes[codeId] != null && code.version != null ) {
                updatedCodes[codeId] shouldNotBe null
                updatedCodes[codeId]!!.containsUpdate(code) shouldBe true
                updatedCodes[codeId]!!.version shouldBe code.version
            }
        }
    }



})