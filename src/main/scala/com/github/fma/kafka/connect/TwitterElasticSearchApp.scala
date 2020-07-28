package com.github.fma.kafka.connect

import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import io.amient.kafka.connect.embedded.ConnectEmbedded
import org.apache.log4j.BasicConfigurator

import scala.jdk.CollectionConverters._

object TwitterElasticSearchApp {
  type JavaMap = java.util.Map[String, String]

  BasicConfigurator.configure()

  val APP_CONFIG: Config = ConfigFactory.load
  val DEFAULT_BOOTSTRAP_SERVER = "localhost:9092"
  val WORKER_PROPS_MAP: JavaMap = Map(
    "bootstrap.servers" -> DEFAULT_BOOTSTRAP_SERVER,
    "key.converter" -> "org.apache.kafka.connect.json.JsonConverter",
    "value.converter" -> "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable" -> "true",
    "value.converter.schemas.enable" -> "true",
    "offset.flush.interval.ms" -> "10000",
    "offset.storage.file.filename" -> "/tmp/connect.offsets",
    "plugin.path" -> "/Users/fma/workspace/kafka-beginners-course/connectors"
  ).asJava

  def main(args: Array[String]): Unit = {
    val workerProps = new Properties()
    val twitterConnectorProps = new Properties()
    val elasticSearchConnectorProps = new Properties()

    workerProps.putAll(WORKER_PROPS_MAP)
    elasticSearchConnectorProps.load(getClass.getClassLoader.getResourceAsStream("elasticsearch.properties"))
    twitterConnectorProps.load(getClass.getClassLoader.getResourceAsStream("twitter.properties"))

    val kafkaConnectEmbedded = new ConnectEmbedded(workerProps,
      Array[Properties](twitterConnectorProps, elasticSearchConnectorProps))

    scala.sys.addShutdownHook({
      kafkaConnectEmbedded.stop()
    })

    kafkaConnectEmbedded.start()

    while(true) {}
  }
}
