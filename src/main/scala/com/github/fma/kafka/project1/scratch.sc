import java.nio.file.{Files, Paths}
import java.time.Instant

import com.danielasfregola.twitter4s.entities.Tweet
import org.json4s.JsonAST.{JObject, JString}
import org.json4s.{CustomSerializer, DefaultFormats, Formats}
import org.json4s.native.JsonMethods
import org.json4s.native.Serialization._

import scala.io.Source

case object InstantSerializer
  extends CustomSerializer[Instant](
    _ =>
      ({
        case JString(s) => Instant.parse(s)
        case JObject(_) => null
      }, Map())
  )

/*
val elasticJsonAsByteArray = Files.readAllLines(
  Paths.get(getClass.getClassLoader.getResource("elasticsearch.json").toURI)).asScala
println(elasticJsonAsByteArray.mkString(""))
 */

implicit val formats: Formats = DefaultFormats + InstantSerializer

val source = Source.fromFile("/tmp/tmp.json")
val json = source.getLines().mkString("")
source.close()

val tweet = JsonMethods.parse(json).extract[Tweet]
println(tweet)
val map = getCCParams(tweet)

