package com.github.fma.kafka.tutorial1

import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory

import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.jdk.DurationConverters._
import scala.language.postfixOps

object ConsumerDemoAssignAndSeek extends App {
  val logger = LoggerFactory.getLogger(getClass)

  val bootstrapServers = "127.0.0.1:9092"
  val topic = "first_topic"

  // create Producer properties
  val properties = new Properties();
  properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
  properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
  properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
  properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  val consumer = new KafkaConsumer[String, String](properties)

  // assign and seek are mostly used to replay data or fetch a specific message
  val partitionToReadFrom = new TopicPartition(topic, 2)
  val offsetToReadFrom = 0L

  consumer.assign(List(partitionToReadFrom).asJava)

  consumer.seek(partitionToReadFrom, offsetToReadFrom)

  val numberOfMsgsToRead = 5

  pollAndStopWhenMsgsRcvd(0)

  logger.info("Exiting the application")

  consumer.close()

  @tailrec
  def pollAndStopWhenMsgsRcvd(acc: Int): Unit = {
    if (acc.equals(numberOfMsgsToRead)) {
      ()
    } else {
      val msgsReceived =
        consumer.poll((100 milliseconds).toJava).asScala.map(record => {
          logger.info(s"Key: ${record.key()}, Value: ${record.value()}")
          logger.info(s"Partition: ${record.partition()}, Offset: ${record.offset()}")
          1
        }).sum
      pollAndStopWhenMsgsRcvd(acc + msgsReceived)
    }
  }
}
