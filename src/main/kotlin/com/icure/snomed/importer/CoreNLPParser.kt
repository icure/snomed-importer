package com.icure.snomed.importer

import edu.stanford.nlp.pipeline.StanfordCoreNLP
import java.util.Properties

class CoreNLPParser(
    val allowedTags: Set<String>
) {

    private val pipeline: StanfordCoreNLP

    init {
        val properties = Properties()
        properties.setProperty("annotators", "tokenize,ssplit,pos")
        pipeline = StanfordCoreNLP(properties)
    }

    fun getTokensByPOSTags(text: String): List<String> {
        val document = pipeline.processToCoreDocument(text)
        return document.tokens()
            .filter {
                allowedTags.contains(it.tag())
            }.map {
                it.word()
            }
    }
}