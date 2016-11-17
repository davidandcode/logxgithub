package com.creditkarma.logx.impl.streamreader

import java.util

import com.creditkarma.logx.core.StreamReader
import com.creditkarma.logx.impl.checkpoint.KafkaCheckpoint
import com.creditkarma.logx.impl.sourcesink.Kafka
import com.creditkarma.logx.impl.streamdata.SparkRDD
import com.creditkarma.logx.utils.LazyLog
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka010.LocationStrategies._
import org.apache.spark.streaming.kafka010.{KafkaUtils, OffsetRange}
import org.scalactic.Fail

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

/**
  * Created by yongjia.wang on 11/16/16.
  */
class KafkaSparkRDDReader[K, V](val source: Kafka)
  extends StreamReader [Kafka, SparkRDD[ConsumerRecord[K, V]], KafkaCheckpoint] with LazyLog {

  var streamingInterval: Long = 1000 // default in msec
  var maxFetchedRecords: Long = 1000 // default records

  override def start(): Boolean = {
    info(s"Starting reader with ${source.kafkaParams}")
    Try(
      new KafkaConsumer[K, V](source.kafkaParams.asJava)
    ) match {
      case Success(kc) =>
        _kafkaConsumer = kc
        true
      case Failure(f) =>
        error(s"Failed to create Kafka consumer: ${source.kafkaParams}")
        false
    }
  }

  override def close(): Unit = {
    if(_kafkaConsumer != null) _kafkaConsumer.close()
  }

  override def fetchData(checkpoint: KafkaCheckpoint): (Boolean, KafkaCheckpoint) = {

    val topicPartitions: Seq[TopicPartition] = _kafkaConsumer.listTopics().asScala.filter {
      case (topic: String, _) => topicFilter(topic)
    }.flatMap(_._2.asScala).map {
      pi => new TopicPartition(pi.topic(), pi.partition())
    }.toSeq

    val checkpointOffsetMap = checkpoint.nextStartingOffset()
    _kafkaConsumer.assign(topicPartitions.asJava) // initialize empty partition offset to 0, otherwise it'll through Exception
    _kafkaConsumer.seekToBeginning(topicPartitions.asJava)
    val topicPartitionStartingOffsetMap: Map[TopicPartition, Long] =
      topicPartitions.map{
        tp =>
          val earliestOffset = _kafkaConsumer.position(tp)
          checkpointOffsetMap.get(tp) match {
            case Some(checkpointOffset) => // the topic partition is checkpointed previously
              if(checkpointOffset < earliestOffset &&
                !checkpoint.fromEarliest) // some offset is missed from the last checkpoint and what is currently available
                warn(s"Missing messages: ${tp}, from $checkpointOffset to $earliestOffset")
              tp -> checkpointOffset
            case None => // a new topic partition
              tp -> earliestOffset
          }
      }.toMap

    // the end of offset range always have the exclusive semantics (starting offset is inclusive)
    _kafkaConsumer.seekToEnd(topicPartitions.asJava)
    _fetchedOffsetRanges =
      topicPartitionStartingOffsetMap.map{
        case (tp: TopicPartition, startingOffset: Long) =>
          OffsetRange(tp, startingOffset, _kafkaConsumer.position(tp))
      }.filter(_.count() > 0).toSeq
    debug(s"Fetched offset ranges: ${_fetchedOffsetRanges}")
    (true, new KafkaCheckpoint(_fetchedOffsetRanges))
  }

  override def flushDownstream(): Boolean = {
    lastFlushTime - System.currentTimeMillis() >= streamingInterval || fetchedRecords >= maxFetchedRecords
  }

  override def fetchedData: SparkRDD[ConsumerRecord[K, V]] = {
    new SparkRDD[ConsumerRecord[K, V]](
      KafkaUtils.createRDD[K, V](
        SparkContext.getOrCreate(), // spark context
        source.kafkaParams.asJava,
        _fetchedOffsetRanges.toArray, //message ranges
        PreferConsistent // location strategy
      )
    )
  }

  /**
    * private internal mutable states
    */
  private var _fetchedOffsetRanges: Seq[OffsetRange] = Seq.empty
  private var _kafkaConsumer: KafkaConsumer[K, V] = null

  /**
    * Kafka reader can be configured to read topics with several approach
    * 1. Specific inclusion/exclusion list
    * 2. Regex
    * 3. Filter method
    * A nicer interface can be exposed later to achieve both flexibility and ease-of-use
    * @param topic
    * @return
    */
  private def topicFilter(topic: String): Boolean = true

  override def fetchedRecords: Long = _fetchedOffsetRanges.map(_.count()).sum
}
