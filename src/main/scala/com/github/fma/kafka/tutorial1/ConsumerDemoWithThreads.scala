package com.github.fma.kafka.tutorial1

import java.util.concurrent.CountDownLatch

import org.slf4j.LoggerFactory

object ConsumerDemoWithThreads extends App {
  val logger = LoggerFactory.getLogger(getClass)

  val bootstrapServers = "127.0.0.1:9092"
  val groupId = "my-sixth-application"
  val topic = "first_topic"

  val latch = new CountDownLatch(1)
  val myConsumerRunnable = new ConsumerRunnable(bootstrapServers, groupId, topic, latch)
  val myThread = new Thread(myConsumerRunnable)

  scala.sys.addShutdownHook({
    logger.info("Caught shutdown hook")
    myConsumerRunnable.shutdown()
    latch.await()
  })

  myThread.start()
  latch.await()
  logger.info("Application is closing")
}
