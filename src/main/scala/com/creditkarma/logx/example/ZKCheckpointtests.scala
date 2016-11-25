package com.creditkarma.logx.example

import com.creditkarma.logx.impl.checkpoint.KafkaCheckpoint
import com.creditkarma.logx.impl.checkpointservice.ZookeeperCheckPointService
import com.creditkarma.logx.utils.gcs.ZooKeeperUnit
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange

import scala.collection.mutable


/**
  * Created by shengwei.wang on 11/24/16.
  */



object ZKCheckpointtests {


  def main(args:Array[String]): Unit ={

     val myZookeeperUnit:ZooKeeperUnit = new ZooKeeperUnit();
    myZookeeperUnit.startService(2181)


    val mySeq: mutable.MutableList[OffsetRange] = new mutable.MutableList[OffsetRange]()

    val topicPartitionSet: mutable.Set[TopicPartition] = new mutable.HashSet[TopicPartition]()
    topicPartitionSet.+=(new TopicPartition("testTopic", 0))
    topicPartitionSet.+=(new TopicPartition("testTopic", 1))
    topicPartitionSet.+=(new TopicPartition("testTopic", 2))
    topicPartitionSet.+=(new TopicPartition("testTopic", 3))
    topicPartitionSet.+=(new TopicPartition("testTopic", 4))

    for (tp <- topicPartitionSet) {


      // fromOffset is set to be 0
      val tempOffsetRange: OffsetRange = OffsetRange.create(tp, 0, 1357924680)
      mySeq.+=(tempOffsetRange)

    }

    val myCp = new KafkaCheckpoint(mySeq)

    val myZookeeperCheckPointService:ZookeeperCheckPointService = new ZookeeperCheckPointService("localhost","zkcp","testTopic",5)
    myZookeeperCheckPointService.commitCheckpoint(myCp)


    val lastCp = myZookeeperCheckPointService.lastCheckpoint()

    for (offsetrange <- lastCp.offsetRanges) {
      assert(1357924680 == offsetrange.untilOffset)
      println("======================= the topic is " + offsetrange.topic + " and the partition is " + offsetrange.partition + " and the committed offset is " + offsetrange.untilOffset + " ================")
    }

  myZookeeperUnit.shutDownService()

  }

}
