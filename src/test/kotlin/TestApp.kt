import com.icure.snomed.importer.createSentenceParser
import com.icure.snomed.importer.makeCodeFromUpdate
import com.icure.snomed.importer.retrieveCodesAndUpdates
import edu.stanford.nlp.international.french.process.FrenchTokenizer
import edu.stanford.nlp.io.IOUtils
import edu.stanford.nlp.pipeline.Annotation
import edu.stanford.nlp.pipeline.CoreDocument
import edu.stanford.nlp.pipeline.StanfordCoreNLP
import edu.stanford.nlp.simple.Document
import edu.stanford.nlp.util.StringUtils
import io.kotest.core.spec.BeforeSpec
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.string.shouldHaveLength
import java.io.File
import java.util.*

class TestApp: StringSpec ({

    val conceptFilename = "src/test/resources/snomed/international/snapshot/test_Concept_Snapshot_INT_20210731.txt"
    val descriptionFilename = "src/test/resources/snomed/international/snapshot/test_Description_Snapshot-en_INT_20210731.txt"
    val relationshipFilename = "src/test/resources/snomed/international/snapshot/test_Relationship_Snapshot_INT_20210731"

    "test" {

//        val props = Properties()
//        props.load(IOUtils.readerFromString("StanfordCoreNLP-french.properties"))
//        props.setProperty("annotators", "tokenize,ssplit,pos")
//        val pipeline = StanfordCoreNLP(props)
//        val ann = pipeline.process("Ceci est mon texte en français. Il contient plusieurs phrases.")
//        val doc = Document(props, ann)
//        val tokens = doc.sentences().fold(setOf<String>()) { acc, sentence ->
//            acc + sentence.words()
//        }
//        println(tokens)
        val annotator = createSentenceParser("fr")
        println(annotator.getTokens("Ceci est mon texte en français. Il contient plusieurs phrases."))
    }
})