package com.creditkarma.logx.impl.sourcesink

import com.creditkarma.logx.core.{Sink, Source}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}

/**
  * Created by yongjia.wang on 11/16/16.
  */
class Kafka(val kafkaParams: Map[String, Object]) extends Source with Sink {
  override def name: String = "kafka"
}

object Kafka {

  // Spark will auto-fix some of the kafka parameters on behalf of the kafka consumers created in each executor thread:
  // receiving TCP buffer size must be >=65536
  // consumer group id is prefixed with spark-executor
  val default = new Kafka(
    Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.serializer" -> classOf[StringSerializer],
      "key.deserializer" -> classOf[StringDeserializer],
      "value.serializer" -> classOf[StringSerializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "logX",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean),
      "receive.buffer.bytes" -> new Integer(10000) // buffer size cannot be smaller than 65536
    ))
}