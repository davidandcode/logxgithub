package com.creditkarma.logx.test

import java.util.Properties

import com.creditkarma.logx.base.Source
import com.creditkarma.logx.impl.checkpoint.KafkaCheckpoint
import com.creditkarma.logx.impl.sourcesink.Kafka
import com.creditkarma.logx.impl.streamreader.KafkaSparkRDDReader
import com.creditkarma.logx.instrumentation.LogInfoInstrumentor
import com.creditkarma.logx.utils.LazyLog
import info.batey.kafka.unit.KafkaUnit
import kafka.admin.AdminUtils
import kafka.producer.KeyedMessage
import kafka.server.{KafkaConfig, KafkaServer}
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.log4j.{ConsoleAppender, Level, LogManager, PatternLayout}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConverters._
/**
  * Created by yongjia.wang on 11/16/16.
  */


object KafkaLocalTest extends LazyLog {

  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "localhost:5558",
    "key.serializer" -> classOf[StringSerializer],
    "key.deserializer" -> classOf[StringDeserializer],
    "value.serializer" -> classOf[StringSerializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "test",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  def main(args: Array[String]): Unit = {

    System.setProperty("java.net.preferIPv4Stack" , "true");
    println(java.net.InetAddress.getLocalHost)
    printLogToConsole()
    setLevel(Level.INFO)

    val kafkaUnitServer = new KafkaUnit(5556, 5558)
    kafkaUnitServer.startup()
    val kc = new KafkaConsumer[String, String](kafkaParams.asJava)
    val kp = new KafkaProducer[String, String](kafkaParams.asJava)
    kafkaUnitServer.createTopic("testTopic", 10)
    Thread.sleep(2000)

    kp.send(new ProducerRecord[String, String]("topicA", "key2", "value3"))
    kp.flush()
    println(kc.listTopics())
    kc.close()

    val kafkaReader =
    new KafkaSparkRDDReader[String, String](new Kafka(kafkaParams))
    kafkaReader.registerInstrumentor(LogInfoInstrumentor)

    println(kafkaReader.start())
    kafkaReader.fetchData(new KafkaCheckpoint())
    println(kafkaReader.fetchedRecords)

    SparkContext.getOrCreate(new SparkConf().setAppName("test").setMaster("local[2]")
      .set("spark.driver.host", "127.0.0.1")
      // set local host explicitly, the call through java.net.InetAddress.getLocalHost on laptop with VPN can be inconsistent
      // Also if it returns IPV6, Spark won't work with it
    )
    println(kafkaReader.fetchedData.rdd.map(_.value()).collect().toSeq)
    info("server shutdown")
    kafkaUnitServer.shutdown()
  }
}
