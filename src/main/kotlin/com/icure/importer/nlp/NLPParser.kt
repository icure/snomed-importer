package com.icure.importer.nlp

import edu.stanford.nlp.io.IOUtils
import edu.stanford.nlp.pipeline.StanfordCoreNLP
import edu.stanford.nlp.simple.Document
import org.springframework.core.io.support.PathMatchingResourcePatternResolver
import java.util.*

interface SentenceParser {
    fun getTokens(text: String): Set<String>
}

fun createSentenceParser(language: String): SentenceParser {
    val posTokens = setOf("CD", "FW", "JJ", "JJR", "JJS", "NN", "NNS", "NNP", "NNPS", "RB", "RBR", "RBS", "VB",
                            "ADJ", "ADV", "NOUN", "PROPN", "VERB", "NUM")
    return when(language) {
        "en" -> CoreNLPParser(posTokens, "english")
        "fr" -> CoreNLPParser(posTokens, "french")
        else -> StopWordsParser(language)
    }
}

class CoreNLPParser(
    private val allowedTags: Set<String>,
    language: String
): SentenceParser {
    private val pipeline: StanfordCoreNLP
    private val properties = Properties()

    init {
        if (language != "english") {
            properties.load(IOUtils.readerFromString("StanfordCoreNLP-$language.properties"))
        }
        properties.setProperty("annotators", "tokenize,ssplit,pos")
        pipeline = StanfordCoreNLP(properties)
    }

    override fun getTokens(text: String): Set<String> {
        val annotation = pipeline.process(text)
        val document = Document(properties, annotation)
        return document.sentences().fold(setOf()) { acc, sentence ->
            acc + sentence.posTags().zip(sentence.words()).fold(setOf()) { innerAcc, tagWord ->
                if (allowedTags.contains(tagWord.first)) innerAcc + tagWord.second.lowercase()
                else innerAcc
            }
        }
    }
}

class StopWordsParser(
    private val language: String
): SentenceParser {

    private val stopwords: Set<String>

    init {
        val resolver = PathMatchingResourcePatternResolver(javaClass.classLoader)
        stopwords = resolver.getResources("classpath*:stopwords/$language.txt")
            .firstOrNull()?.file?.readLines()?.toSet()
                ?: throw IllegalStateException("No stopwords found for language $language")
    }

    override fun getTokens(text: String): Set<String> {
        return text.lowercase()
            .replace("[^\\w\\s]".toRegex(), " ")
            .split(" ")
            .filter { it ->
                it.isNotEmpty() && !stopwords.contains(it)
            }
            .toSet()
    }
}