package com.icure.importer.utils

import com.icure.importer.nlp.SentenceParser
import com.icure.importer.nlp.createSentenceParser
import org.springframework.stereotype.Component

@Component
class MultiLanguageParser {

    private val parsers: MutableMap<String, SentenceParser> = mutableMapOf()

    fun getTokens(language: String, sentence: String) =
        parsers[language]?.getTokens(sentence)
            ?: parsers.put(language, createSentenceParser(language))?.getTokens(sentence)
            ?: setOf()

}