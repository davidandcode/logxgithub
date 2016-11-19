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

  val offsetRangesMap: collection.mutable.Map[TopicPartition, OffsetRange] =
    collection.mutable.Map.empty ++ offsetRanges.map{osr=>osr.topicPartition()->osr}

  override def fromEarliest: Boolean = offsetRanges.isEmpty

  def nextStartingOffset(): Map[TopicPartition, Long] = {
    allOffsetRanges.map{
      osr => osr.topicPartition() -> osr.untilOffset
    }.toMap
  }

  def allOffsetRanges = offsetRangesMap.values

  // for kafka offset ranges, topic partition with 0 records are not included, and therefore must be accumulated with all the previous offsetranges
  def mergeKafkaCheckpoint(checkpoint: KafkaCheckpoint): KafkaCheckpoint = {
    for(offsetRange <- checkpoint.allOffsetRanges){
      offsetRangesMap.get(offsetRange.topicPartition()) match {
        case Some(existingOffsetRange) =>
          if(existingOffsetRange.untilOffset != offsetRange.fromOffset){
            throw new Exception(s"New offsetRange not connected\nNew:${offsetRange}\nExisting:${existingOffsetRange}")
          }
          offsetRangesMap(offsetRange.topicPartition()) = offsetRange
        case None => offsetRangesMap += offsetRange.topicPartition()->offsetRange
      }
    }
    this
  }

  override def toString = offsetRanges.toString()
}
