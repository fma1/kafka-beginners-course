package com.github.fma.kafka.project1

import java.nio.file.{Files, Paths}
import java.util.Properties
import java.util.concurrent.Executors

import com.danielasfregola.twitter4s.entities.Tweet
import com.github.fma.kafka.project1.Utils._
import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s._
import com.sksamuel.elastic4s.http.{JavaClient, NoOpRequestConfigCallback}
import com.sksamuel.elastic4s.requests.common.RefreshPolicy
import com.sksamuel.elastic4s.requests.searches.SearchResponse
import org.apache.http.auth.{AuthScope, UsernamePasswordCredentials}
import org.apache.http.impl.client.BasicCredentialsProvider
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder
import org.apache.kafka.clients.consumer.ConsumerConfig._
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.elasticsearch.client.RestClientBuilder.HttpClientConfigCallback
import org.json4s._
import org.json4s.JObject
import org.json4s.JsonAST.JString
import org.json4s.native.Serialization._
import org.json4s.native.JsonMethods._
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}
import scala.jdk.CollectionConverters._
import scala.jdk.DurationConverters._
import scala.language.postfixOps

object ElasticSearchConsumer {
  final val logger: Logger = LoggerFactory.getLogger(getClass)

  implicit val executionContext: ExecutionContextExecutor =
    ExecutionContext.fromExecutor(Executors.newFixedThreadPool(2))
  implicit val formats: Formats = DefaultFormats + InstantSerializer

  def createElasticClient(): ElasticClient = {
    val elasticJsonString = Files.readAllLines(
      Paths.get(getClass.getClassLoader.getResource("elasticsearch.json").toURI)).asScala.mkString("")
    val jsonData = parse(elasticJsonString).asInstanceOf[JObject] match { case JObject(obj) => obj.toMap }
    val elasticUsername: String = jsonData(ELASTIC_USERNAME) match { case JString(str) => str }
    val elasticPassword: String = jsonData(ELASTIC_PASSWORD) match { case JString(str) => str }
    val httpClientConfigCallback: HttpClientConfigCallback = (httpClientBuilder: HttpAsyncClientBuilder) => {
      val creds = new BasicCredentialsProvider()
      creds.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(elasticUsername, elasticPassword))
      httpClientBuilder.setDefaultCredentialsProvider(creds)
    }

    ElasticClient(JavaClient(ElasticProperties(ELASTIC_URL), NoOpRequestConfigCallback, httpClientConfigCallback))
  }

  def createKafkaConsumer(topic: String): KafkaConsumer[String, String] = {
    val bootstrapServers = "127.0.0.1:9092"
    val groupId = "kafka-demo-elasticsearch"

    val properties = new Properties()
    properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    properties.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    properties.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    properties.setProperty(GROUP_ID_CONFIG, groupId)
    properties.setProperty(AUTO_OFFSET_RESET_CONFIG, "earliest")

    val consumer = new KafkaConsumer[String, String](properties)
    consumer.subscribe(List(topic).asJava)

    consumer
  }

  def main(args: Array[String]): Unit = {
    val consumer = createKafkaConsumer(KAFKA_TOPIC)
    val elasticClient = createElasticClient()

    scala.sys.addShutdownHook({ logger.info("Shutdown hook invoked") })

    Future {
      while (true) {
        val records = consumer.poll((100 milliseconds).toJava).asScala

        for (record <- records) {
          val tweet: Tweet = parse(record.value()).extract[Tweet]
          val tweetId: String = tweet.id.toString
          val tweetMap = getCCParams(tweet)

          elasticClient.execute {
            bulk (
              indexInto("twitter").fields(tweetMap)
            ).refresh(RefreshPolicy.WAIT_FOR)
          }.await

          val response: Response[SearchResponse] = elasticClient.execute {
            search("twitter").matchQuery("id", tweetId.toString)
          }.await

          response match {
            case failure: RequestFailure => println(s"We failed: ${failure.error}")
            case results: RequestSuccess[SearchResponse] => println(results.result.hits.hits.head.id)
            case results: RequestSuccess[_] => println(results.result)
            case _ => println("default case")
          }

        }

      }
    }

  }
}
