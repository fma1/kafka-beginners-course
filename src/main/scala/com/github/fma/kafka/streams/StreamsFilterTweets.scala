package com.github.fma.kafka.streams

import java.util.Properties

import com.fasterxml.jackson.databind.{DeserializationFeature, JsonNode, ObjectMapper, PropertyNamingStrategy}
import com.github.fma.kafka.project1.InstantSerializer
import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.apache.kafka.common.serialization.Serdes.StringSerde
import org.apache.kafka.connect.json.{JsonDeserializer, JsonSerializer}
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.{KafkaStreams, StreamsBuilder, StreamsConfig}
import org.apache.log4j.BasicConfigurator
import org.json4s.{DefaultFormats, Formats}

import scala.concurrent.duration._
import scala.jdk.DurationConverters._
import scala.language.postfixOps

object StreamsFilterTweets {
  val BOOTSTRAP_SERVER = "127.0.0.1:9092"
  val FROM_TOPIC = "twitter_status_connect"
  val TO_TOPIC = "important_tweets"

  implicit val formats: Formats = DefaultFormats + InstantSerializer

  def main(args: Array[String]): Unit = {
    BasicConfigurator.configure()

    val jsonObjectMapper = new ObjectMapper
    val jsonSerde: Serde[JsonNode] = Serdes.serdeFrom(new JsonSerializer(), new JsonDeserializer())
    jsonObjectMapper.setPropertyNamingStrategy(PropertyNamingStrategy.UPPER_CAMEL_CASE)
    jsonObjectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    val config: Properties = {
      val p = new Properties()
      p.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-filter-tweets")
      p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER)
      p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, classOf[StringSerde])
      p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, classOf[StringSerde])
      p
    }

    val builder = new StreamsBuilder

    builder.stream(FROM_TOPIC, Consumed.`with`(jsonSerde, jsonSerde))
      .filter((_, jsonNode) => jsonNode.get("payload").get("Retweet").asBoolean())
      .to(TO_TOPIC)

    val streams: KafkaStreams = new KafkaStreams(builder.build(), config)
    streams.start()

    sys.ShutdownHookThread {
      streams.close((10 seconds).toJava)
    }
  }
}
