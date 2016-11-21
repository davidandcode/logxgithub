package com.creditkarma.logx.impl.checkpointservice

/**
  * Created by shengwei.wang on 11/19/16.
  * This version, thread unsafe! But do we need it to be thread safe?
  * Do we need multiple threads to use the same KafkaCheckpointService object concurrently>
  * Could create one KafkaCheckpointService object per thread though
  */


import com.creditkarma.logx.base.CheckpointService
import com.creditkarma.logx.impl.checkpoint.KafkaCheckpoint
import org.apache.kafka.clients.consumer.{KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange

import scala.collection.mutable
import scala.collection.JavaConversions._



class KafkaCheckpointService(kafkaParams:Map[String,Object],topic:String = null,numPartition:Int = 0) extends CheckpointService[KafkaCheckpoint] {


  val kc = new KafkaConsumer[String, String](kafkaParams)

  override def commitCheckpoint(cp: KafkaCheckpoint): Unit = {


    val topicPartitionOffsetMap:Map[TopicPartition, Long] = cp.nextStartingOffset()
    val numOfTopicPartitions = topicPartitionOffsetMap.size
    val topicPartitionSet:Set[TopicPartition] = topicPartitionOffsetMap.keySet

    if(numOfTopicPartitions <= 0){
      throw new Exception("No toptic partions found with this KafkaCheckpoint");
    }

    val offsetsToCommit:mutable.HashMap[TopicPartition, OffsetAndMetadata]= new mutable.HashMap[TopicPartition,OffsetAndMetadata]()

    for(tp <- topicPartitionSet){
      val tempOffset = topicPartitionOffsetMap(tp)
      val tempOffsetAndMetadata = new OffsetAndMetadata(tempOffset)
      offsetsToCommit.+=(tp ->tempOffsetAndMetadata)
    }


    kc.commitSync(offsetsToCommit)


  }

  override def lastCheckpoint(): KafkaCheckpoint = {


    var newTopicPartitionSet:mutable.HashSet[TopicPartition] = new mutable.HashSet[TopicPartition]()

    // if nothing is committed last time, return null
    if(topic == null || numPartition <=0) {
      return null
    }else {

      for(i <- 0 until numPartition){
        newTopicPartitionSet.+=(new TopicPartition(topic,i))
      }

    }


    val mySeq:mutable.MutableList[OffsetRange] = new mutable.MutableList[OffsetRange]()

    for(tp <- newTopicPartitionSet){

      val tempOffset:Long = kc.committed(tp).offset

      // fromOffset is set to be 0
      val tempOffsetRange:OffsetRange = OffsetRange.create(tp,0,tempOffset)
      mySeq.+=(tempOffsetRange)

    }

    new KafkaCheckpoint(mySeq)

  }
}
