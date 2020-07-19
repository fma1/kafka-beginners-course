package com.github.fma.kafka.project1

import Constants._
import java.util.Properties

import com.danielasfregola.twitter4s.TwitterStreamingClient
import com.danielasfregola.twitter4s.entities.Tweet
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.clients.producer.ProducerConfig._
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory

object TwitterProducer extends App {
  val logger = LoggerFactory.getLogger(getClass)

  val kafkaProducerProps: Properties = {
    val props = new Properties()
    props.put(BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
    props.put(KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.put(VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    // Safe Producer
    props.put(ENABLE_IDEMPOTENCE_CONFIG, true)
    props.put(ACKS_CONFIG, "all")
    props.put(RETRIES_CONFIG, Int.MaxValue)
    // Kafka 2.5 >= 1.1 so we can keep this here
    props.put(MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5)
    props.put(COMPRESSION_TYPE_CONFIG, "snappy")
    props.put(LINGER_MS_CONFIG, "20")
    props.put(BATCH_SIZE_CONFIG, 32*1024)
    props
  }
  val producer = new KafkaProducer[String, String](kafkaProducerProps)

  val twitterClient = TwitterStreamingClient()
  twitterClient.sampleStatuses(stall_warnings = true)({
    case tweet: Tweet =>
      println(s"Sending tweet ${tweet.id}")
      producer.send(new ProducerRecord[String, String](KAFKA_TOPIC, tweet.id.toString, tweet.toString),
        (_: RecordMetadata, e: Exception) => {
          Option(e).foreach(_ => logger.info("Something bad happened", e))
        })
      producer.flush()
  })

  while (true) {
    Thread.sleep(2000)
  }
  producer.close()
}
