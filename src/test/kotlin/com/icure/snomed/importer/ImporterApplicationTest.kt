package com.icure.snomed.importer

import TestFileGenerator
import com.icure.snomed.importer.snomed.retrieveCodesAndUpdates
import com.icure.snomed.importer.utils.*
import io.icure.kraken.client.apis.CodeApi
import io.icure.kraken.client.models.CodeDto
import io.icure.kraken.client.models.ListOfIdsDto
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.collections.shouldContain
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import kotlinx.coroutines.ExperimentalCoroutinesApi
import java.io.File

fun CodeDto.containsUpdate(other: CodeUpdate): Boolean {
    return this.code == other.code &&
            this.regions.intersect(other.regions).isNotEmpty() &&
            (other.disabled == null || this.disabled == other.disabled) &&
            other.description.all { this.label != null && this.label!![it.key] == it.value } &&
            other.relationsRemove.all {
                this.qualifiedLinks[it.key] == null ||
                        (this.qualifiedLinks[it.key] != null && this.qualifiedLinks[it.key]!!.all { existingLink ->
                            !it.value.contains(existingLink)
                        })
            } &&
            other.relationsAdd.all {
                this.qualifiedLinks[it.key] != null &&
                        it.value.all { existingLink ->
                            this.qualifiedLinks[it.key]!!.contains(existingLink)
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
    val numSamples = 400
    val codeApi = CodeApi(basePath = "http://127.0.0.1:16043", authHeader = basicAuth(System.getenv("USERNAME"), System.getenv("PASSWORD")))

    // Creates some simulate Snapshot and Delta files and initializes a database with some codes
    beforeSpec {
        val tfg = TestFileGenerator()
        File(conceptFilename).printWriter().use {
            it.print(tfg.generateConcepts(numSamples, false))
        }
        File(conceptDeltaFilename).printWriter().use {
            it.print(tfg.generateConcepts(numSamples, true, numSamples/2))
        }
        File(descriptionFilename).printWriter().use {
            it.print(tfg.generateDescriptions(numSamples, false))
        }
        File(descriptionDeltaFilename).printWriter().use {
            it.print(tfg.generateDescriptions(numSamples, true, numSamples/2))
        }
        File(relationshipFilename).printWriter().use {
            it.print(tfg.generateRelationships(numSamples, false))
        }
        File(relationshipDeltaFilename).printWriter().use {
            it.print(tfg.generateRelationships(numSamples, true, numSamples/2))
        }

        val conceptFile = File(conceptFilename)
        val descriptionFiles = setOf(File(descriptionFilename))
        val relationshipFile = File(relationshipFilename)

        val codes = retrieveCodesAndUpdates("int", conceptFile, descriptionFiles, relationshipFile)

        val insertionResult = batchDBUpdate(
            codes,
            "SNOMED",
            100,
            codeApi,
            CommandlineProgressBar("Initializing codes...", codes.size)
        ).let {
            codeApi.getCodes(ListOfIdsDto(it))
        }
        println("")

        insertionResult.size shouldBe numSamples
    }

    "Adding and updating codes in the DB" {
        val databaseStatus = codeApi.findCodesByType(
            region = null,
            type = null,
            code = null,
            version = null,
            startKey = null,
            startDocumentId = null,
            limit = 100000
        ).rows.groupBy { it.code!! }

        val conceptFile = File(conceptDeltaFilename)
        val descriptionFiles = setOf(File(descriptionDeltaFilename))
        val relationshipFile = File(relationshipDeltaFilename)

        val codes = retrieveCodesAndUpdates("int", conceptFile, descriptionFiles, relationshipFile)

        val updateResult = batchDBUpdate(
            codes,
            "SNOMED",
            100,
            codeApi,
            CommandlineProgressBar("Updating codes...", codes.size)
        ).let {
            codeApi.getCodes(ListOfIdsDto(it))
        }
        println("")

        val updatedCodes = updateResult.associateBy { it.code!! }

        codes.forEach { (codeId, code) ->
            // If the code does not exist in the DB but is a dummy, then it is not added
            if (databaseStatus[codeId] == null && code.version == null) {
                updatedCodes[codeId] shouldBe null
            }
            // If the code does not exist in the DB and is valid, it is added
            else if (databaseStatus[codeId] == null && code.version != null) {
                updatedCodes[codeId] shouldNotBe null
                updatedCodes[codeId]!!.containsUpdate(code) shouldBe true
                updatedCodes[codeId]!!.version shouldBe code.version
            }
            // If the code exists but the update doesn't specify a new version,
            // the code with the latest version is updated
            else if (databaseStatus[codeId] != null && code.version == null) {
                updatedCodes[codeId] shouldNotBe null
                updatedCodes[codeId]!!.containsUpdate(code) shouldBe true
                updatedCodes[codeId]!!.version shouldBe databaseStatus[codeId]!!.maxByOrNull { it.version!!.lowercase() }?.version
            }
            // If the code exists and the update specifies a new version,
            // the code is updated with a new version
            else if (databaseStatus[codeId] != null && code.version != null && databaseStatus[codeId]!!.none { it.version == code.version }) {
                updatedCodes[codeId] shouldNotBe null
                updatedCodes[codeId]!!.containsUpdate(code) shouldBe true
                updatedCodes[codeId]!!.version shouldBe code.version
            }
            // If the code exists and the update specifies an existing version,
            // the code with that version is updated
            else if (databaseStatus[codeId] != null && code.version != null && databaseStatus[codeId]!!.any { it.version == code.version }) {
                updatedCodes[codeId] shouldNotBe null
                updatedCodes[codeId]!!.containsUpdate(code) shouldBe true
                updatedCodes[codeId]!!.version shouldBe code.version
            }
        }
    }

    "Check that update operation is idempotent" {
        val conceptFile = File(conceptDeltaFilename)
        val descriptionFiles = setOf(File(descriptionDeltaFilename))
        val relationshipFile = File(relationshipDeltaFilename)

        val codes = retrieveCodesAndUpdates("int", conceptFile, descriptionFiles, relationshipFile)

        val firstUpdate = batchDBUpdate(
            codes,
            "SNOMED",
            100,
            codeApi,
            CommandlineProgressBar("Updating codes...", codes.size)
        ).let {
            codeApi.getCodes(ListOfIdsDto(it))
        }.associateBy { it.code!! }
        println("")

        val secondUpdate = batchDBUpdate(
            codes,
            "SNOMED",
            100,
            codeApi,
            CommandlineProgressBar("Updating codes...", codes.size)
        ).let {
            codeApi.getCodes(ListOfIdsDto(it))
        }.associateBy { it.code!! }
        println("")

        secondUpdate.size shouldBe firstUpdate.size

        secondUpdate.forEach { (key, second) ->
            firstUpdate[key] shouldNotBe null
            val first = firstUpdate[key]!!
            first.id shouldBe second.id
            second.regions.forEach { region ->
                first.regions shouldContain region
            }
            second.qualifiedLinks.forEach{ (type, links) ->
                first.qualifiedLinks[type] shouldNotBe null
                first.qualifiedLinks[type]!!.size shouldBe links.size
                links.forEach { link -> first.qualifiedLinks[type]!! shouldContain link }
            }
            second.searchTerms.forEach { (lang, terms) ->
                first.searchTerms[lang] shouldNotBe null
                first.searchTerms[lang]!!.size shouldBe terms.size
                terms.forEach { term -> first.searchTerms[lang]!! shouldContain term }
            }
            first.disabled shouldBe second.disabled
            first.type shouldBe second.type
            first.code shouldBe second.code
            first.version shouldBe second.version
        }
    }

})