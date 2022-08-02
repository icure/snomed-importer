import org.apache.xpath.operations.Bool
import kotlin.random.Random.Default.nextInt

class TestFileGenerator {

    private val alphabet: List<Char> = ('a' .. 'z').toList() + ('A'..'Z') + ('0'..'9') + listOf('!','"','£','$','%','&','/','(',')','=','?','\\', '\'')
    private val languages = listOf("en", "fr", "nl")
    private val relationshipTypes = listOf("116680003", "116680004", "116680005", "116680006", "166680005", "416680005", "446680005", "116440005")

    data class RelationshipAccumulator(
        val generated: Set<Set<String>>,
        val fileContent: String
    )

    private fun generateRandomString(length: Int) = (1..length)
        .map { _ -> alphabet[nextInt(0, alphabet.size)] }
        .joinToString("");

    private fun generateRandomDate() = "20${nextInt(0,3)}${nextInt(0,10)}0131"

    fun generateConcepts(rows: Int, delta: Boolean, offset: Int = 0) = (1+offset..rows+offset)
        .fold("id\teffectiveTime\tactive\tmoduleId\tdefinitionStatusId") { str, it ->
            if (delta && nextInt(0,2) == 1) str
            else str + "\n0$it\t${generateRandomDate()}\t${nextInt(0,2)}\t0000\t0000"
        }

    fun generateDescriptions(rows: Int, delta: Boolean, offset: Int = 0) = (1+offset..rows+offset)
        .fold("id\teffectiveTime\tactive\tmoduleId\tconceptId\tlanguageCode\ttypeId\tterm\tcaseSignificanceId") { str, it ->
            if (delta && nextInt(0,2) == 1) str
            else str +
            "\n0${it}1\t${generateRandomDate()}\t1\t0000\t0${it}\t${languages[nextInt(0,3)]}\t900000000000013009\t${generateRandomString(nextInt(10, 101))}\t0000" +
            "\n0${it}2\t${generateRandomDate()}\t1\t0000\t0${it}\t${languages[nextInt(0,3)]}\t900000000000003001\t${generateRandomString(nextInt(10, 101))}\t0000" +
            "\n0${it}3\t${generateRandomDate()}\t0\t0000\t0${it}\t${languages[nextInt(0,3)]}\t900000000000013009\t${generateRandomString(nextInt(10, 101))}\t0000" +
            "\n0${it}4\t${generateRandomDate()}\t0\t0000\t0${it}\t${languages[nextInt(0,3)]}\t900000000000003001\t${generateRandomString(nextInt(10, 101))}\t0000"
    }

    fun generateRelationships(rows: Int, delta: Boolean, offset: Int = 0): String {
        val finalAccumulator = (offset..(rows * 10)+offset)
            .fold(
                RelationshipAccumulator(
                    setOf(),
                    "id\teffectiveTime\tactive\tmoduleId\tsourceId\tdestinationId\trelationshipGroup\ttypeId\tcharacteristicTypeId\tmodifierId"
                )
            ) { acc, it ->
                if (delta && nextInt(0,2) == 1) acc
                else {
                    var randRel: String
                    var sourceId: String
                    var destinationId: String
                    do {
                        randRel = relationshipTypes[nextInt(0, 3)]
                        sourceId = nextInt(1, rows).toString()
                        destinationId = nextInt(1, rows).toString()
                    } while (acc.generated.contains(setOf(randRel, sourceId, destinationId)))

                    RelationshipAccumulator(
                        acc.generated + setOf(setOf(randRel, sourceId, destinationId)),
                        acc.fileContent + "\n0$it\t${generateRandomDate()}\t${
                            nextInt(
                                0,
                                2
                            )
                        }\t0000\t0$sourceId\t0$destinationId\t0000\t$randRel\t0000\t0000"
                    )
                }
            }
        return finalAccumulator.fileContent
    }
}