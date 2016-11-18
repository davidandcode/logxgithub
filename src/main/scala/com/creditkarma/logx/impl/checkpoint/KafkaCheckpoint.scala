package com.creditkarma.logx.impl.checkpoint

import com.creditkarma.logx.base.Checkpoint
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange

/**
  * For Kafka, the checkpoint is the end of each offsetRange, which will be used as stating point to construct new offsetRanges
  * The reader is responsible for correctly interpreting the checkpoint, and generating new checkpoint for next batch
  * @param offsetRanges
  */
class KafkaCheckpoint(offsetRanges: Seq[OffsetRange] = Seq.empty) extends Checkpoint{

  override def fromEarliest: Boolean = offsetRanges.isEmpty

  def nextStartingOffset(): Map[TopicPartition, Long] = {
    offsetRanges.map{
      osr => osr.topicPartition() -> osr.untilOffset
    }.toMap
  }

  override def toString = offsetRanges.toString()
}
