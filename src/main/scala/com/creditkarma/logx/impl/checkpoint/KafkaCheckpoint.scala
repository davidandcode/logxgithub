package com.creditkarma.logx.impl.checkpoint

import com.creditkarma.logx.base.Checkpoint
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange

/**
  * For Kafka, the checkpoint is the end of each offsetRange, which will be used as stating point to construct new offsetRanges
  * The reader is responsible for correctly interpreting the checkpoint, and generating new checkpoint for next batch
  * @param offsetRanges
  */
class KafkaCheckpoint(val offsetRanges: Seq[OffsetRange] = Seq.empty) extends Checkpoint[Seq[OffsetRange], KafkaCheckpoint]{

  def nextStartingOffset(): Map[TopicPartition, Long] = {
    offsetRanges.map{
      osr => osr.topicPartition() -> osr.untilOffset
    }.toMap
  }

  override def toString = offsetRanges.mkString(", ")

  override def mergeDelta(delta: Seq[OffsetRange]): KafkaCheckpoint = {
    val offsetRangesMap = collection.mutable.Map.empty[TopicPartition, OffsetRange] ++
      offsetRanges.map{
        osr => osr.topicPartition() -> osr
      }.toMap

    for(offsetRange <- delta){
      offsetRangesMap.get(offsetRange.topicPartition()) match {
        case Some(existingOffsetRange) =>
          if(existingOffsetRange.untilOffset != offsetRange.fromOffset){
            throw new Exception(s"OffsetRanges not connected\nNew:${offsetRange}\nExisting:${existingOffsetRange}")
          }
          offsetRangesMap(offsetRange.topicPartition()) =
            OffsetRange(offsetRange.topicPartition(), existingOffsetRange.fromOffset, offsetRange.untilOffset)
        case None => offsetRangesMap += offsetRange.topicPartition()->offsetRange
      }
    }
    new KafkaCheckpoint(offsetRangesMap.values.toSeq)
  }
}
