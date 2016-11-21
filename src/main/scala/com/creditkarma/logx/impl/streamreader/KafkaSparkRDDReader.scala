package com.creditkarma.logx.impl.streamreader

import com.creditkarma.logx.base.{Reader, StatusError, StatusOK}
import com.creditkarma.logx.impl.checkpoint.KafkaCheckpoint
import com.creditkarma.logx.impl.streambuffer.SparkRDD
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkContext
import org.apache.spark.streaming.kafka010.LocationStrategies._
import org.apache.spark.streaming.kafka010.{KafkaUtils, OffsetRange}

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

/**
  * Created by yongjia.wang on 11/16/16.
  */
class KafkaSparkRDDReader[K, V](val kafkaParams: Map[String, Object])
  extends Reader[SparkRDD[ConsumerRecord[K, V]], KafkaCheckpoint, Seq[OffsetRange], Seq[OffsetRange]] {

  private var flushInterval: Long = 1000 // default in msec
  private var maxFetchedRecordsPerPartition: Long = 1000 // default records

  def setMaxFetchRecordsPerPartition(n: Long) = {
    maxFetchedRecordsPerPartition = Math.max(1, n) // cannot be 0
    this
  }

  def setFlushInterval(t: Long) = {
    flushInterval = t
    this
  }

  def kafkaConsumer: KafkaConsumer[K, V] = {
    if (_kafkaConsumer == null) {
      statusUpdate(this, new StatusOK(s"Creating Kafka consumer with ${kafkaParams}"))
      Try(
        new KafkaConsumer[K, V](kafkaParams.asJava)
      ) match {
        case Success(kc) =>
          _kafkaConsumer = kc
        case Failure(f) =>
          statusUpdate(this, new StatusError(new Exception(s"Failed to create Kafka consumer: ${kafkaParams}", f)))
      }
    }
    _kafkaConsumer
  }

  override def close(): Unit = {

    if(_kafkaConsumer != null) {
      statusUpdate(this, new StatusOK(s"Closing kafka consumer in reader $this"))
      _kafkaConsumer.close()
    }
  }

  override def fetchData(lastFlushTime: Long, checkpoint: KafkaCheckpoint): (SparkRDD[ConsumerRecord[K, V]], Seq[OffsetRange]) = {

    val topicPartitions: Seq[TopicPartition] = kafkaConsumer.listTopics().asScala.filter {
      case (topic: String, _) => topicFilter(topic)
    }.flatMap(_._2.asScala).map {
      pi => new TopicPartition(pi.topic(), pi.partition())
    }.toSeq

    statusUpdate(this, new StatusOK(s"Got topic partitions ${topicPartitions}"))

    val checkpointOffsetMap = checkpoint.nextStartingOffset()
    kafkaConsumer.assign(topicPartitions.asJava) // initialize empty partition offset to 0, otherwise it'll through Exception
    kafkaConsumer.seekToBeginning(topicPartitions.asJava)
    val topicPartitionStartingOffsetMap: Map[TopicPartition, Long] =
      topicPartitions.map{
        tp =>
          val earliestOffset = kafkaConsumer.position(tp)
          checkpointOffsetMap.get(tp) match {
            case Some(checkpointOffset) => // the topic partition is checkpointed previously
              if(checkpointOffset < earliestOffset) // some offset is missed from the last checkpoint and what is currently available
                {
                  statusUpdate(this, new StatusError(new Exception(s"Missing messages: ${tp}, from $checkpointOffset to $earliestOffset")))
                }

              tp -> checkpointOffset
            case None => // a new topic partition
              tp -> earliestOffset
          }
      }.toMap

    // the end of offset range always have the exclusive semantics (starting offset is inclusive)
    kafkaConsumer.seekToEnd(topicPartitions.asJava)
    val fetchedOffsetRanges =
      topicPartitionStartingOffsetMap.map{
        case (tp: TopicPartition, startingOffset: Long) =>
          val endPosition = kafkaConsumer.position(tp)
          OffsetRange(
            tp, startingOffset,
            Math.min(startingOffset + maxFetchedRecordsPerPartition, endPosition)
          )
      }.filter(_.count() > 0).toSeq

    statusUpdate(this, new StatusOK(s"Fetched offset ranges: ${fetchedOffsetRanges}"))

    (new SparkRDD[ConsumerRecord[K, V]](
        KafkaUtils.createRDD[K, V](
          SparkContext.getOrCreate(), // spark context
          kafkaParams.asJava,
          fetchedOffsetRanges.toArray, //message ranges
          PreferConsistent // location strategy
        )
      ),
      fetchedOffsetRanges
      )
  }

  /*override def flushDownstream(): Boolean = {
    System.currentTimeMillis() - lastFlushTime >= flushInterval || fetchedRecords >= maxFetchedRecords
  }*/


  /**
    * private internal mutable states
    */
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
  private def topicFilter(topic: String): Boolean = {
    topic.indexOf("__consumer_offsets") == -1
  }

  //override def fetchedRecords: Long = if(_fetchedOffsetRanges.isEmpty) 0 else _fetchedOffsetRanges.map(_.count()).sum
  /**
    * This is about streaming flush policy, can be based on data size, time interval or combination
    *
    * @return whether the currently fetched/buffered data should be flushed down the stream
    */
  override def flush(lastFlushTime: Long, meta: Seq[OffsetRange]): Boolean = {
    System.currentTimeMillis() - lastFlushTime >= flushInterval || inRecords(meta) >= maxFetchedRecordsPerPartition
  }

  override def inRecords(meta: Seq[OffsetRange]): Long = {
    if(meta.isEmpty) 0 else meta.map(_.count()).sum
  }

  override def inBytes(meta: Seq[OffsetRange]): Long = {
    statusUpdate(this, new StatusOK("SparkRDD read bytes info is not available at read time (this is expected)"))
    0
  }

  // for this reader, read meta is the same as read delta,
  // but in general meta data is superset of delta
  override def getDelta(meta: Seq[OffsetRange]): Seq[OffsetRange] = meta
}
