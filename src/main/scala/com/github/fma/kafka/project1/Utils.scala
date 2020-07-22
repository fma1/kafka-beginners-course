package com.github.fma.kafka.project1

object Utils {
  val KAFKA_TOPIC = "twitter_tweets"
  val ELASTIC_USERNAME = "username"
  val ELASTIC_PASSWORD = "password"
  val ELASTIC_URL = "https://kafka-poc-testing-6994796743.us-east-1.bonsaisearch.net:443"

  def getCCParams(cc: AnyRef): Map[String, String] =
    cc.getClass.getDeclaredFields.foldLeft(Map.empty[String, String]) { (a, f) =>
      f.setAccessible(true)
      a + (f.getName ->
        Option(f.get(cc)).map(_.toString).orNull)
    }
}
