package com.github.fma.kafka.tutorial1

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

object ProducerDemo extends App {
  val bootstrapServers = "127.0.0.1:9092"

  // create Producer properties
  val properties = new Properties();
  properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
  properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
  properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

  // create a producer
  val producer = new KafkaProducer[String, String](properties);

  // create a producer record
  val record = new ProducerRecord[String, String]("first_topic", "hello_world")

  // send data
  producer.send(record)

  // flush data
  producer.flush()

  // flush and close producer
  producer.close()
}
