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
import org.apache.http.auth.{AuthScope, UsernamePasswordCredentials}
import org.apache.http.impl.client.BasicCredentialsProvider
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer
import org.elasticsearch.client.RestClientBuilder.HttpClientConfigCallback
import org.json4s._
import org.json4s.JObject
import org.json4s.JsonAST.JString
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
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

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

        val tweetTupleLst = records.map(record => {
          val tweet: Tweet = parse(record.value()).extract[Tweet]
          val tweetId: String = tweet.id.toString
          val tweetMap = getCCParams(tweet)
          (tweetId, tweetMap)
        })

        val tweetIndexRequestLst =
          tweetTupleLst.map { case (tweetId, tweetMap) => indexInto(ELASTIC_INDEX).id(tweetId).fields(tweetMap) }

        val tweetSearchRequestLst =
          tweetTupleLst.map { case (tweetId, _) => search(ELASTIC_INDEX).matchQuery("id", tweetId) }

        logger.info("Bulk inserting twitter requests...")

        elasticClient.execute {
          bulk(tweetIndexRequestLst)
            .refresh(RefreshPolicy.WAIT_FOR)
        }.await

        elasticClient.execute {
          multi(tweetSearchRequestLst)
        }.await.toOption.get.successes
          .foreach(searchResponse => logger.info(s"Id: ${searchResponse.hits.hits.head.id}"))

        elasticClient.execute {
          multi(tweetSearchRequestLst)
        }.await match {
          case RequestSuccess(_, _, _, result) =>
            result.successes.foreach(searchResponse =>
              logger.info(s"Successfully found id ${searchResponse.hits.hits.head.id}"))
            if (result.failures.nonEmpty) logger.error(s"Number of errors: ${result.failures.size}")
        }

      }
    }

  }
}
