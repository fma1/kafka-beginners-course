package com.github.fma.kafka.tutorial1

import java.util.Properties

import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory

object ProducerDemoKeys extends App {
  val logger = LoggerFactory.getLogger(getClass)

  val bootstrapServers = "127.0.0.1:9092"
  val callback = new Callback {
    override def onCompletion(recordMetadata: RecordMetadata, e: Exception): Unit = {
      Option(e) match {
        case Some(_) =>
          logger.error("Error while producing", e)
        case None =>
          logger.info(s"""Received new metadata:
                         |Topic: ${recordMetadata.topic()}
                         |Partition: ${recordMetadata.partition()}
                         |Offset: ${recordMetadata.offset()}
                         |Timestamp: ${recordMetadata.timestamp()}""")
      }
    }
  }

  // create Producer properties
  val properties = new Properties();
  properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
  properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
  properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

  // create a producer
  val producer = new KafkaProducer[String, String](properties);

  (0 to 9).foreach(idx => {
    val topic = "first_topic"
    val value = s"hello_world $idx"
    val key = s"id_$idx"

    logger.info(s"Key: $key")
    // id_0 -> partition 1
    // id_1 -> partition 0
    // id_2 -> partition 2

    // create a producer record
    val record = new ProducerRecord[String, String](topic, key, value)

    // send data
    producer.send(record, callback).get() // block send to make synchronous - don't do this in production!
  })

  // flush data
  producer.flush()

  // flush and close producer
  producer.close()
}
