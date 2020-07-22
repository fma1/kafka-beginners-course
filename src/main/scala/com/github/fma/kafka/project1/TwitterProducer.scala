package com.github.fma.kafka.project1

import Utils._
import java.util.Properties

import com.danielasfregola.twitter4s.TwitterStreamingClient
import com.danielasfregola.twitter4s.entities.Tweet
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.clients.producer.ProducerConfig._
import org.apache.kafka.common.serialization.StringSerializer
import org.json4s._
import org.json4s.native.Serialization
import org.json4s.native.Serialization._
import org.slf4j.{Logger, LoggerFactory}

import scala.sys.addShutdownHook

object TwitterProducer {
  val logger: Logger = LoggerFactory.getLogger(getClass)

  def createKafkaProducer(): KafkaProducer[String, String] = {
    val kafkaProducerProps = new Properties()
    kafkaProducerProps.put(BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
    kafkaProducerProps.put(KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    kafkaProducerProps.put(VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    // Safe Producer
    kafkaProducerProps.put(ENABLE_IDEMPOTENCE_CONFIG, true)
    kafkaProducerProps.put(ACKS_CONFIG, "all")
    kafkaProducerProps.put(RETRIES_CONFIG, Int.MaxValue)
    // Kafka 2.5 >= 1.1 so we can keep this here
    kafkaProducerProps.put(MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5)
    kafkaProducerProps.put(COMPRESSION_TYPE_CONFIG, "snappy")
    kafkaProducerProps.put(LINGER_MS_CONFIG, "20")
    kafkaProducerProps.put(BATCH_SIZE_CONFIG, 32*1024)
    new KafkaProducer[String, String](kafkaProducerProps)
  }

  def main(args: Array[String]): Unit = {
    val kafkaProducer = createKafkaProducer()
    val twitterClient = TwitterStreamingClient()

    addShutdownHook({
      logger.info("Closing Kafka Producer...")
      kafkaProducer.close()
      logger.info("Exiting TwitterProducer...")
    })

    twitterClient.sampleStatuses(stall_warnings = true)({
      case tweet: Tweet =>
        logger.info(s"Sending tweet ${tweet.id}")
        val tweetJson = write(tweet)(Serialization.formats(NoTypeHints))
        kafkaProducer.send(new ProducerRecord[String, String](KAFKA_TOPIC, tweet.id.toString, tweetJson),
          (_: RecordMetadata, e: Exception) => {
            Option(e).foreach(_ => logger.info("Something bad happened", e))
          })
        kafkaProducer.flush()
    })
  }
}
