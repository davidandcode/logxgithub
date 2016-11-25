package com.creditkarma.logx.impl.checkpointservice

import com.creditkarma.logx.base.{Checkpoint, CheckpointService}
import com.creditkarma.logx.impl.checkpoint.KafkaCheckpoint
import com.creditkarma.logx.utils.gcs.{ByteUtils, ZookeeperCpUtils}
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import org.apache.zookeeper.ZooKeeper

import scala.collection.mutable

/**
  * Created by shengwei.wang on 11/19/16.
  */
class ZookeeperCheckPointService(hostport:String,path:String,topic:String = null,numPartition:Int = 0) extends  CheckpointService[KafkaCheckpoint] {



  override def commitCheckpoint(cp: KafkaCheckpoint): Unit = {


    val topicPartitionOffsetMap:Map[TopicPartition, Long] = cp.nextStartingOffset()
    val numOfTopicPartitions = topicPartitionOffsetMap.size
    val topicPartitionSet:Set[TopicPartition] = topicPartitionOffsetMap.keySet

    require(numOfTopicPartitions > 0)

    val offsetsToCommit:mutable.HashMap[TopicPartition, OffsetAndMetadata]= new mutable.HashMap[TopicPartition,OffsetAndMetadata]()

    for(tp <- topicPartitionSet){
      val tempOffset = topicPartitionOffsetMap(tp)
      val tempOffsetAndMetadata = new OffsetAndMetadata(tempOffset)
      offsetsToCommit.+=(tp ->tempOffsetAndMetadata)
    }


    for(tp <- offsetsToCommit.keySet){

   val tempPathString:String = "/" + path + tp.topic() +  tp.partition()


       if(!ZookeeperCpUtils.znodeExists(tempPathString,hostport)){
         ZookeeperCpUtils.create(tempPathString,offsetsToCommit(tp).offset(),hostport)
       }else{
         ZookeeperCpUtils.update(tempPathString,offsetsToCommit(tp).offset(),hostport)
       }

    }






  }

  override def lastCheckpoint(): KafkaCheckpoint = {

    var newTopicPartitionSet:mutable.HashSet[String] = new mutable.HashSet[String]()
    val mySeq:mutable.MutableList[OffsetRange] = new mutable.MutableList[OffsetRange]()

    // if nothing is committed last time, return null
    require(topic != null && numPartition > 0 && path != null)


      for(i <- 0 until numPartition) {

        //val number:Long = ZookeeperCpUtils.getData("/" + path + "/" + topic + "/" + i,hostport)
        val number:Long = ZookeeperCpUtils.getData("/" + path + topic + i,hostport)

        // fromOffset is set to be 0
        val tempOffsetRange:OffsetRange = OffsetRange.create(topic,i,0,number)
        mySeq.+=(tempOffsetRange)

      }







    new KafkaCheckpoint(mySeq)


  }

}
