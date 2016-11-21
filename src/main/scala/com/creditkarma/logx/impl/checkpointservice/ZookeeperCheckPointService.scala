package com.creditkarma.logx.impl.checkpointservice

import com.creditkarma.logx.base.{Checkpoint, CheckpointService}
import com.creditkarma.logx.impl.checkpoint.KafkaCheckpoint
import com.creditkarma.logx.utils.gcs.ZookeeperCpUtils
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import org.apache.zookeeper.ZooKeeper

import scala.collection.mutable

/**
  * Created by shengwei.wang on 11/19/16.
  */
class ZookeeperCheckPointServiceextends(hostport:String,path:String,topic:String = null,numPartition:Int = 0) extends  CheckpointService[KafkaCheckpoint] {



  override def commitCheckpoint(cp: KafkaCheckpoint): Unit = {


    val topicPartitionOffsetMap:Map[TopicPartition, Long] = cp.nextStartingOffset()
    val numOfTopicPartitions = topicPartitionOffsetMap.size
    val topicPartitionSet:Set[TopicPartition] = topicPartitionOffsetMap.keySet

    // if(numOfTopicPartitions <= 0){
    // throw new Exception("No toptic partions found with this KafkaCheckpoint");
    //}

    require(numOfTopicPartitions > 0)

    val offsetsToCommit:mutable.HashMap[TopicPartition, OffsetAndMetadata]= new mutable.HashMap[TopicPartition,OffsetAndMetadata]()

    for(tp <- topicPartitionSet){
      val tempOffset = topicPartitionOffsetMap(tp)
      val tempOffsetAndMetadata = new OffsetAndMetadata(tempOffset)
      offsetsToCommit.+=(tp ->tempOffsetAndMetadata)
    }


    for(tp <- offsetsToCommit.keySet){
    val tempPathString:String = "/" + path + "/" + tp.topic() + "/" + tp.partition()


    }


   // kc.commitSync(offsetsToCommit)



  }

  override def lastCheckpoint(): KafkaCheckpoint = {

    var newTopicPartitionSet:mutable.HashSet[TopicPartition] = new mutable.HashSet[TopicPartition]()

    // if nothing is committed last time, return null
    require(topic != null && numPartition > 0 && path != null)


      for(i <- 0 until numPartition){
        newTopicPartitionSet.+=(new TopicPartition(topic,i))
      }




    val mySeq:mutable.MutableList[OffsetRange] = new mutable.MutableList[OffsetRange]()

    for(tp <- newTopicPartitionSet){

      //val tempOffset:Long = kc.committed(tp).offset

      val zk:ZooKeeper = ZookeeperCpUtils.getAZookeeper(path)
      val ckArray:Array[Byte] =ZookeeperCpUtils.getData(path,zk)


      // fromOffset is set to be 0
    //  val tempOffsetRange:OffsetRange = OffsetRange.create(tp,0,tempOffset)
     // mySeq.+=(tempOffsetRange)

    }

    new KafkaCheckpoint(mySeq)


  }

}
