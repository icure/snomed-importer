package com.icure.snomed.importer

import TestFileGenerator
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.collections.shouldContain
import io.kotest.matchers.collections.shouldNotContain
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import org.springframework.core.io.support.PathMatchingResourcePatternResolver
import java.io.File

class ReleaseFileParserTest : StringSpec({

    val resolver = PathMatchingResourcePatternResolver(javaClass.classLoader)

    val conceptFilename = "build/tmp/test/test_Concept_Snapshot_INT_20210731.txt"
    val conceptDeltaFilename = "build/tmp/test/test_Concept_Delta_INT_20210731.txt"
    val descriptionFilename = "build/tmp/test/test_Description_Snapshot-en_INT_20210731.txt"
    val descriptionDeltaFilename = "build/tmp/test/test_Description_Delta-en_INT_20210731.txt"
    val relationshipFilename = "build/tmp/test/test_Relationship_Snapshot_INT_20210731"
    val relationshipDeltaFilename = "build/tmp/test/test_Relationship_Delta_INT_20210731"
    val emptyFilename = "build/tmp/test/empty.txt"
    val numSamples = 100

    beforeSpec {
        val tfg = TestFileGenerator()
        File(conceptFilename).printWriter().use {
            it.print(tfg.generateConcepts(numSamples, false))
        }
        File(conceptDeltaFilename).printWriter().use {
            it.print(tfg.generateConcepts(numSamples, true))
        }
        File(descriptionFilename).printWriter().use {
            it.print(tfg.generateDescriptions(numSamples, false))
        }
        File(descriptionDeltaFilename).printWriter().use {
            it.print(tfg.generateDescriptions(numSamples, true))
        }
        File(relationshipFilename).printWriter().use {
            it.print(tfg.generateRelationships(numSamples, false))
        }
        File(relationshipDeltaFilename).printWriter().use {
            it.print(tfg.generateRelationships(numSamples, true))
        }
        File(emptyFilename).printWriter().use {
            it.print("")
        }
    }

    "The conceptId, version, and active field are correctly read from the concept file" {
        val conceptFile = File(conceptFilename)
        val tmpFile = File(emptyFilename)
        conceptFile shouldNotBe null
        tmpFile shouldNotBe null
        val codes = retrieveCodesAndUpdates("int", conceptFile, setOf(), tmpFile)
        val remainingCodes = conceptFile.readLines()
            .fold(codes) { acc, it ->
                val (id, version, active, _, _) = it.split('\t')
                if (id != "id") {
                    codes[id]?.let {
                        val tmpCode = makeCodeFromUpdate(it, "SNOMED")
                        tmpCode.version shouldNotBe null
                        tmpCode.version shouldBe version
                        tmpCode.disabled shouldBe (active == "0")
                    } shouldNotBe null
                }
                acc - id
            }
        remainingCodes.isEmpty() shouldBe true
    }

    "Each active Fully Qualified Name in the description file is added to the corresponding code" {
        val conceptFile = File(conceptFilename)
        val descriptionFiles = setOf(File(descriptionFilename))
        val tmpFile = File(emptyFilename)
        conceptFile shouldNotBe null
        tmpFile shouldNotBe null
        descriptionFiles.isNotEmpty() shouldBe true
        val codes = retrieveCodesAndUpdates("int", conceptFile, descriptionFiles, tmpFile)

        val remainingCodes = File(descriptionFilename).readLines()
            .fold(codes) { acc, it ->
                val (_, _, active, _, conceptId, language, typeId, term, _) = it.split("\t")
                if (conceptId != "id" && active == "1" && typeId == "900000000000003001") {
                    codes[conceptId]?.let {
                        val tmpCode = makeCodeFromUpdate(it, "SNOMED")
                        tmpCode.label?.let { label ->
                            label.isNotEmpty() shouldBe true
                            label[language] shouldNotBe null
                            label[language] shouldBe term
                        } shouldNotBe null
                    } shouldNotBe null
                    acc - conceptId
                } else acc
            }
        remainingCodes.isEmpty() shouldBe true
    }

    "The International Snapshot release should not have active descriptions that correspond to a non-existing code" {
        val conceptFile = resolver.getResources("classpath*:/snomed/international/snapshot/sct2_Concept_**.txt").firstOrNull()?.file
        val descriptionFiles = resolver.getResources("classpath*:/snomed/international/snapshot/sct2_Description_**.txt")
            .fold(setOf<File>()) { map, it ->
                map + it.file
            }
        conceptFile shouldNotBe null
        descriptionFiles.isNotEmpty() shouldBe true
        val codesSet = mutableSetOf<String>()
        conceptFile!!.forEachLine {
            val (conceptId, _, _, _, _) = it.split("\t")
            if (conceptId != "id") {
                codesSet.add(conceptId)
            }
        }
        descriptionFiles.forEach {file ->
            file.forEachLine {
                val (_, _, active, _, conceptId, _, _, _, _) = it.split("\t")
                if (active == "1") codesSet shouldContain conceptId
            }
        }
    }

    "All the active relationships in the Relationship file should be assigned to the corresponding code" {
        val conceptFile = File(conceptFilename)
        val relationshipFile = File(relationshipFilename)
        conceptFile shouldNotBe null
        relationshipFile shouldNotBe null

        val codes = retrieveCodesAndUpdates("int", conceptFile, setOf(), relationshipFile)
        File(relationshipFilename).readLines()
            .fold(codes) { acc, it ->
                val (_, _, active, _, sourceId, destinationId, _, typeId, _, _) = it.split("\t")
                if (sourceId != "id" && active == "1") {
                    codes[sourceId]?.let {
                        val tmpCode = makeCodeFromUpdate(it, "SNOMED")
                        tmpCode.qualifiedLinks.isNotEmpty() shouldBe true
                        tmpCode.qualifiedLinks[typeId]?.let {links ->
                            links shouldContain destinationId
                        } shouldNotBe null
                    } shouldNotBe null
                }
                acc
            }
    }

    "All the inactive relationships in the Relationship file should not be assigned to the corresponding code" {
        val conceptFile = File(conceptFilename)
        val relationshipFile = File(relationshipFilename)
        conceptFile shouldNotBe null
        relationshipFile shouldNotBe null

        val codes = retrieveCodesAndUpdates("int", conceptFile, setOf(), relationshipFile)
        File(relationshipFilename).readLines()
            .fold(codes) { acc, it ->
                val (_, _, active, _, sourceId, destinationId, _, typeId, _, _) = it.split("\t")
                if (sourceId != "id" && active == "0") {
                    codes[sourceId]?.let {
                        val tmpCode = makeCodeFromUpdate(it, "SNOMED")
                        if(tmpCode.qualifiedLinks.isNotEmpty() && tmpCode.qualifiedLinks[typeId] != null) {
                            tmpCode.qualifiedLinks[typeId]!!.contains(destinationId) shouldBe false
                        }
                    } shouldNotBe null
                }
                acc
            }
    }

    "The International Snapshot release should not have active relationships that correspond to a non existing code" {
        val conceptFile = resolver.getResources("classpath*:/snomed/international/snapshot/sct2_Concept_**.txt").firstOrNull()?.file
        val relationshipFile = resolver.getResources("classpath*:/snomed/international/snapshot/sct2_Relationship_**.txt").firstOrNull()?.file

        conceptFile shouldNotBe null
        relationshipFile shouldNotBe null
        val codesSet = mutableSetOf<String>()
        conceptFile!!.forEachLine {
            val (conceptId, _, _, _, _) = it.split("\t")
            if (conceptId != "id") {
                codesSet.add(conceptId)
            }
        }
        relationshipFile!!.forEachLine {
            val (_, _, active, _, sourceId, destinationId, _, _, _, _) = it.split("\t")
            if (active == "1"){
                codesSet shouldContain sourceId
                codesSet shouldContain destinationId
            }
        }
    }

    "The conceptId, version, and active field of existing codes are correctly updated from the delta concepts file" {
        val conceptFile = File(conceptFilename)
        val tmpFile = File(emptyFilename)
        val conceptDeltaFile = File(conceptDeltaFilename)
        conceptFile shouldNotBe null
        conceptDeltaFile shouldNotBe null
        tmpFile shouldNotBe null
        val updates = retrieveCodesAndUpdates("int", conceptDeltaFile, setOf(), tmpFile)
        retrieveCodesAndUpdates("int", conceptFile, setOf(), tmpFile)
            .map{ (k, v) -> (k to makeCodeFromUpdate(v, "SNOMED"))}
            .forEach {
                updates[it.first]?.let {codeUpdate ->
                    val tmpCode = makeCodeFromUpdate(codeUpdate, "SNOMED", it.second)
                    tmpCode.code shouldBe codeUpdate.code
                    tmpCode.code shouldBe it.second.code
                    tmpCode.version shouldBe codeUpdate.version
                    tmpCode.disabled shouldBe codeUpdate.disabled
                }
            }
    }

    "The descriptions are correctly updated from the delta description file" {
        val conceptFile = File(conceptFilename)
        val descriptionFiles = setOf(File(descriptionFilename))
        val tmpFile = File(emptyFilename)
        val conceptDeltaFile = File(conceptDeltaFilename)
        val descriptionDeltaFiles = setOf(File(descriptionDeltaFilename))
        conceptFile shouldNotBe null
        conceptDeltaFile shouldNotBe null
        tmpFile shouldNotBe null
        descriptionFiles.isNotEmpty() shouldBe true
        descriptionDeltaFiles.isNotEmpty() shouldBe true
        val updates = retrieveCodesAndUpdates("int", conceptDeltaFile, descriptionDeltaFiles, tmpFile)
        retrieveCodesAndUpdates("int", conceptFile, descriptionFiles, tmpFile)
            .map{ (k, v) -> (k to makeCodeFromUpdate(v, "SNOMED"))}
            .forEach { it ->
                updates[it.first]?.let {codeUpdate ->
                    val tmpCode = makeCodeFromUpdate(codeUpdate, "SNOMED", it.second)
                    tmpCode.code shouldBe codeUpdate.code
                    tmpCode.code shouldBe it.second.code
                    if(codeUpdate.version != null) {
                        tmpCode.version shouldBe codeUpdate.version
                        tmpCode.disabled shouldBe codeUpdate.disabled
                    } else {
                        tmpCode.version shouldBe it.second.version
                        tmpCode.disabled shouldBe it.second.disabled
                    }
                    codeUpdate.description.forEach { (k, _) ->
                        tmpCode.label?.let {
                            it[k] shouldNotBe null
                        } shouldNotBe null
                    }
                    it.second.label?.let {
                        it.forEach { (k, _) ->
                            tmpCode.label?.let { label ->
                                label[k] shouldNotBe null
                            } shouldNotBe null
                        }
                    } shouldNotBe null
                    println(tmpCode)
                    println(it.second)
                    println(codeUpdate)
                    tmpCode.label?.let { labelMap ->
                        labelMap.forEach { (k, v) ->
                            codeUpdate.description[k]?.let { desc ->
                                v shouldBe desc
                            } ?: (v shouldBe it.second.label?.get(k))
                        }
                    }
                }
            }
    }

    "All the active relationships in the delta relationships file should be added to the code" {
        val conceptFile = File(conceptFilename)
        val relationshipFile = File(relationshipFilename)
        conceptFile shouldNotBe null
        relationshipFile shouldNotBe null

        val conceptDeltaFile = File(conceptDeltaFilename)
        val relationshipDeltaFile = File(relationshipDeltaFilename)
        conceptDeltaFile shouldNotBe null
        relationshipDeltaFile shouldNotBe null

        val updates = retrieveCodesAndUpdates("int", conceptDeltaFile, setOf(), relationshipDeltaFile)
        retrieveCodesAndUpdates("int", conceptFile, setOf(), relationshipFile)
            .map{ (k, v) -> (k to makeCodeFromUpdate(v, "SNOMED"))}
            .forEach { pair ->
                updates[pair.first]?.let { codeUpdate ->
                    val tmpCode = makeCodeFromUpdate(codeUpdate, "SNOMED", pair.second)
                    tmpCode.code shouldBe codeUpdate.code
                    tmpCode.code shouldBe pair.second.code
                    if(codeUpdate.version != null) {
                        tmpCode.version shouldBe codeUpdate.version
                        tmpCode.disabled shouldBe codeUpdate.disabled
                    } else {
                        tmpCode.version shouldBe pair.second.version
                        tmpCode.disabled shouldBe pair.second.disabled
                    }
                    tmpCode.qualifiedLinks.forEach { (k, v) ->
                        v.forEach{ link ->
                            codeUpdate.relationsRemove[k]?.let {
                                it shouldNotContain link
                            }
                            val linksActive = (codeUpdate.relationsAdd[k] ?: listOf()) + (pair.second.qualifiedLinks[k] ?: listOf())
                            linksActive shouldContain link
                        }
                    }
                }
            }
    }

})
