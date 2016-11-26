package com.creditkarma.logx.impl.transformer

import com.creditkarma.logx.base.{StatusOK, Transformer}
import com.creditkarma.logx.impl.streambuffer.SparkRDD
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange

import scala.collection.mutable

/**
  * Created by yongjia.wang on 11/18/16.
  */
case class KafkaTimePartitionedMessage(topic: String, partition: Int, timePartition: String, message: String)
trait TopicPartitionByIndex {
  def apply(index: Int): TopicPartition
}
trait TimePartitionParser {
  def getTimePartition(message: String): String
}
class KafkaSparkStringRDDTimePartitionTransformer
(topicPartitionByIndex: TopicPartitionByIndex, timePartitionParser: TimePartitionParser)
  extends Transformer[SparkRDD[ConsumerRecord[String, String]], SparkRDD[KafkaTimePartitionedMessage],Seq[OffsetRange]] {
  override def transform(input: SparkRDD[ConsumerRecord[String, String]]): (SparkRDD[KafkaTimePartitionedMessage],Seq[OffsetRange]) = {

    val transformedOffsetRanges:Seq[OffsetRange] = new mutable.LinkedList[OffsetRange]


    (new SparkRDD(
      input.rdd.mapPartitionsWithIndex{
        case (partitionIndex, consumerRecords) =>
          consumerRecords.map{
            cr =>
              val tp = topicPartitionByIndex(partitionIndex)
              KafkaTimePartitionedMessage(tp.topic(), tp.partition(), timePartitionParser.getTimePartition(cr.value()), cr.value())
          }

      }
    ),transformedOffsetRanges)
  }



  override def outRecords(meta: Seq[OffsetRange]): Long = {
    if(meta.isEmpty) 0 else meta.map(_.count()).sum
  }

  override def outBytes(meta: Seq[OffsetRange]): Long = {
    statusUpdate(this, new StatusOK("bytes info of output is not available at transformation time"))
    0
  }


  override def inRecords(meta: Seq[OffsetRange]): Long = {
    if(meta.isEmpty) 0 else meta.map(_.count()).sum
  }

  override def inBytes(meta: Seq[OffsetRange]): Long = {
    statusUpdate(this, new StatusOK("bytes info of input is not available at transformation time"))
    0
  }
}
