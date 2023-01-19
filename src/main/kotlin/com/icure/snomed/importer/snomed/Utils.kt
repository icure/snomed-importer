package com.icure.snomed.importer.snomed

import com.icure.snomed.importer.nlp.SentenceParser
import com.icure.snomed.importer.nlp.createSentenceParser
import com.icure.snomed.importer.utils.*
import java.io.File

// Adds the codes from a delta release
fun retrieveCodesAndUpdates(
    region: String,
    conceptFile: File,
    descriptionFiles: Set<File>,
    relationshipFile: File
): Map<String, CodeUpdate> {
    val parsers = mutableMapOf<String, SentenceParser>()    // Parsers in different languages to elaborate search terms
    val codes = sortedMapOf<String, CodeUpdate>(compareBy{ it.lowercase() })

    // First, I parse all the concepts
    val conceptsBar = CommandlineProgressBar("Parsing codes")
    conceptFile.forEachLine {
        conceptsBar.print()
        conceptsBar.step()
        val (conceptId, conceptVersion, active, _, _) = it.split("\t")
        if (conceptId != "id"){
            codes[conceptId] = CodeUpdate(conceptId, mutableSetOf(region), conceptVersion, active == "0")
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
                if (codes[conceptId] == null) codes[conceptId] = CodeUpdate(conceptId, mutableSetOf(region))

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
            if (codes[sourceId] == null) codes[sourceId] = CodeUpdate(sourceId, mutableSetOf(region))

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