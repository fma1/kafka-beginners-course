package com.github.fma.kafka.tutorial1

import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerConfig._
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.jdk.DurationConverters._
import scala.language.postfixOps

object ConsumerDemo extends App {
  val logger = LoggerFactory.getLogger(getClass)

  val bootstrapServers = "127.0.0.1:9092"
  val groupId = "my-fourth-application"
  val topic = "first_topic"

  // create Producer properties
  val properties = new Properties();
  properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
  properties.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
  properties.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
  properties.setProperty(GROUP_ID_CONFIG, groupId)
  properties.setProperty(AUTO_OFFSET_RESET_CONFIG, "earliest")

  val consumer = new KafkaConsumer[String, String](properties)

  consumer.subscribe(List(topic).asJava)

  while(true) {
    consumer.poll((100 milliseconds).toJava).asScala.foreach(record => {
      logger.info(s"Key: ${record.key()}, Value: ${record.value()}")
      logger.info(s"Partition: ${record.partition()}, Offset: ${record.offset()}")
    })
  }

  consumer.close()
}
