package com.github.fma.kafka.project1

import java.nio.file.{Files, Paths}
import java.util.Properties
import java.util.concurrent.Executors

import com.github.fma.kafka.project1.Constants._
import com.sksamuel.elastic4s._
import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.http.{JavaClient, NoOpRequestConfigCallback}
import com.sksamuel.elastic4s.requests.searches.SearchResponse
import org.apache.http.auth.{AuthScope, UsernamePasswordCredentials}
import org.apache.http.impl.client.BasicCredentialsProvider
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.ConsumerConfig._
import org.apache.kafka.common.serialization.StringSerializer
import org.elasticsearch.client.RestClientBuilder.HttpClientConfigCallback
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.jdk.DurationConverters._
import scala.language.postfixOps

object ElasticSearchConsumer extends App {
  val logger = LoggerFactory.getLogger(getClass)

  implicit val executionContext: ExecutionContextExecutor =
    ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor())

  val kafkaConsumerProps: Properties = {
    val props = new Properties()
    props.put(BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
    props.put(KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.put(VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props
  }
  val consumer = new KafkaConsumer[String, String](kafkaConsumerProps)

  lazy val elasticJsonAsByteArray= Files.readAllBytes(
    Paths.get(getClass.getClassLoader.getResource("elasticsearch.json").toURI))
  lazy val jsonData = ujson.read(ujson.Readable.fromByteArray(elasticJsonAsByteArray))
  lazy val elasticUsername = jsonData.obj(ELASTIC_USERNAME).str
  lazy val elasticPassword = jsonData.obj(ELASTIC_PASSWORD).str

  val httpClientConfigCallback =  new HttpClientConfigCallback {
    override def customizeHttpClient(httpClientBuilder: HttpAsyncClientBuilder): HttpAsyncClientBuilder = {
      val creds = new BasicCredentialsProvider()
      creds.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(elasticUsername, elasticPassword))
      httpClientBuilder.setDefaultCredentialsProvider(creds)
    }
  }

  val elasticClient = ElasticClient(
    JavaClient(
      ElasticProperties("https://kafka-poc-testing-6994796743.us-east-1.bonsaisearch.net:443"),
      NoOpRequestConfigCallback,
      httpClientConfigCallback
    )
  )

  elasticClient.execute {
    bulk (
      indexInto("twitter").fields("foo" -> "bar", "type" -> "tweet")
    ).refreshImmediately
  }.await

  val response: Response[SearchResponse] = elasticClient.execute {
    search("twitter").matchQuery("foo", "bar")
  }.await

  response match {
    case failure: RequestFailure => println(s"We failed: ${failure.error}")
    case results: RequestSuccess[SearchResponse] => println(results.result.hits.hits.head)
    case results: RequestSuccess[_] => println(results.result)
    case _ => println("default case")
  }

  consumer.subscribe(List(KAFKA_TOPIC).asJava)

  scala.sys.addShutdownHook({ logger.info("Caught shutdown hook") })

  Future {
    while (true) {
      val records = consumer.poll((100 milliseconds).toJava).asScala

      for (record <- records) {
        logger.info(s"Key: ${record.key()}, Value: ${record.value()}")
        logger.info(s"Partition: ${record.partition()}, Offset: ${record.offset()}")
      }
    }
  }

  elasticClient.close()
}
