package com.creditkarma.logx.example

/**
  * Created by shengwei.wang on 11/21/16.
  */


import java.io.{BufferedReader, FileReader}

import com.creditkarma.logx.base.{BufferedData, CheckpointService, Transformer, Writer, _}
import com.creditkarma.logx.impl.checkpoint.KafkaCheckpoint
import com.creditkarma.logx.impl.checkpointservice.KafkaCheckpointService
import com.creditkarma.logx.impl.checkpointservice.ZookeeperCheckPointService
import com.creditkarma.logx.impl.streambuffer.SparkRDD
import info.batey.kafka.unit.KafkaUnit
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.streaming.kafka010.OffsetRange
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.collection.JavaConversions._

/**
  * Created by shengwei.wang on 11/17/16.
  */
object KafkaCheckpointtests {


  class InMemoryKafkaCheckpointService extends CheckpointService[KafkaCheckpoint]{
    var lastCheckPoint: KafkaCheckpoint = new KafkaCheckpoint()
    override def commitCheckpoint(cp: KafkaCheckpoint): Unit = {
      lastCheckPoint = cp
    }

    override def lastCheckpoint(): KafkaCheckpoint = {
      lastCheckPoint
    }
  }

  val (zkPort, kafkaPort) = (5556, 5558)
  val shengweiport = 1818

  object TestKafkaServer {

    val kafkaUnitServer = new KafkaUnit(zkPort, kafkaPort)
    val kp = new KafkaProducer[String, String](kafkaParams.asJava)

    def start(): Unit = {
      kafkaUnitServer.startup()
      kafkaUnitServer.createTopic("testTopic", 5)
    }

    val testData: ListBuffer[String] = ListBuffer.empty
    var dataSentToKafka = 0

    def loadData (dataFile: String): Unit = {
      val kp = new KafkaProducer[String, String](kafkaParams.asJava)
      val br = new BufferedReader(new FileReader(dataFile))
      while({

        val line = br.readLine()
        kp.send(new ProducerRecord[String, String]("testTopic", null, line))
        kp.flush()
        line != null
      }){
      }

      kp.close()
    }

    def sendNextMessage(n: Int): Unit = {
      var i = 0
      while(i < n && sendNextMessage()){
        i += 1
      }
    }

    def sendNextMessage(): Boolean = {
      if(dataSentToKafka == testData.size){
        false
      }
      else {
        kp.send(new ProducerRecord[String, String]("testTopic", null, testData(dataSentToKafka)))
        dataSentToKafka += 1
        true
      }
    }

    def sentMessages: Seq[String] = testData.slice(0, dataSentToKafka)

    def stop(): Unit = {
      kp.close()
      kafkaUnitServer.shutdown()
    }
  }

  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> s"localhost:$kafkaPort",
    "key.serializer" -> classOf[StringSerializer],
    "key.deserializer" -> classOf[StringDeserializer],
    "value.serializer" -> classOf[StringSerializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "test"
  )

  def main(args: Array[String]): Unit = {



      TestKafkaServer.start()
      TestKafkaServer.loadData("test_data/KRSOffer.json")

      LogManager.getLogger("org.apache").setLevel(Level.WARN)
      LogManager.getLogger("kafka").setLevel(Level.WARN)


      SparkContext.getOrCreate(new SparkConf().setAppName("test").setMaster("local[2]")
        .set("spark.driver.host", "127.0.0.1")
        // set local host explicitly, the call through java.net.InetAddress.getLocalHost on laptop with VPN can be inconsistent
        // Also if it returns IPV6, Spark won't work with it
      )



      ////////////////////////////////////

      // test on case when the last committed checkpoint is not in this thread and is from some other thread
      // but you need to provide topic name and number of topics

      // the following is to simulate the case when the spark process has collapsed and prior to its collapse, the checkpoint is saved
      // and we are trying to read out the saved checkpoint

    try{
      val testTopic0: TopicPartition = new TopicPartition("testTopic",0)
      val testTopic1: TopicPartition = new TopicPartition("testTopic",1)
      val testTopic2: TopicPartition = new TopicPartition("testTopic",2)
      val testTopic3: TopicPartition = new TopicPartition("testTopic",3)
      val testTopic4: TopicPartition = new TopicPartition("testTopic",4)

      val offsets: Map[TopicPartition,OffsetAndMetadata] =  Map[TopicPartition,OffsetAndMetadata](
        testTopic0 -> new OffsetAndMetadata(456),
        testTopic1 -> new OffsetAndMetadata(456),
        testTopic2 -> new OffsetAndMetadata(456),
        testTopic3 -> new OffsetAndMetadata(456),
        testTopic4 -> new OffsetAndMetadata(456)
      )



      val kc = new KafkaConsumer[String, String](kafkaParams)

      kc.commitSync(offsets)

      val testKafkaCheckpointService = new KafkaCheckpointService(kafkaParams,"testTopic",5)

      val lastCpOtherThread = testKafkaCheckpointService.lastCheckpoint()

      for (offsetrange <- lastCpOtherThread.offsetRanges) {
        assert(456 == offsetrange.untilOffset)
        println("Other thread ======================= the topic is " + offsetrange.topic + " and the partition is " + offsetrange.partition + " and the committed offset is " + offsetrange.untilOffset + " ================")
      }


      /////////////////////////////////////////////
      // now in the same thread, check in a point and read it out

      val mySeq: mutable.MutableList[OffsetRange] = new mutable.MutableList[OffsetRange]()

      val topicPartitionSet: mutable.Set[TopicPartition] = new mutable.HashSet[TopicPartition]()
      topicPartitionSet.+=(new TopicPartition("testTopic", 0))
      topicPartitionSet.+=(new TopicPartition("testTopic", 1))
      topicPartitionSet.+=(new TopicPartition("testTopic", 2))
      topicPartitionSet.+=(new TopicPartition("testTopic", 3))
      topicPartitionSet.+=(new TopicPartition("testTopic", 4))

      for (tp <- topicPartitionSet) {


        // fromOffset is set to be 0
        val tempOffsetRange: OffsetRange = OffsetRange.create(tp, 0, 888)
        mySeq.+=(tempOffsetRange)

      }

      val myCp = new KafkaCheckpoint(mySeq)


      // now use the standard api to save check point
      testKafkaCheckpointService.commitCheckpoint(myCp)


      val lastCp = testKafkaCheckpointService.lastCheckpoint()

      for (offsetrange <- lastCp.offsetRanges) {
        assert(888 == offsetrange.untilOffset)
        println("======================= the topic is " + offsetrange.topic + " and the partition is " + offsetrange.partition + " and the committed offset is " + offsetrange.untilOffset + " ================")
      }


//val CRAP:Array[String] = new Array[String](2)
  // ZKCreate.main(CRAP)

      //ZookeeperCpUtils.create("shengwei",1712,"localhost")
      //val myLong:Long = ZookeeperCpUtils.getData("shengwei","localhost")

      //println("hello my long is " + myLong)

      //val testZookeeperCheckpointService: ZookeeperCheckPointService= new ZookeeperCheckPointService("localhost:" + zkPort,"zkcp","testTopic",5)
      //testZookeeperCheckpointService.commitCheckpoint(myCp)
     // val zkCp = testZookeeperCheckpointService.lastCheckpoint()
/*
      for (offsetrange <- zkCp.offsetRanges) {
        assert(888 == offsetrange.untilOffset)
        println(" This is From ZooKeeper ! ======================= the topic is " + offsetrange.topic + " and the partition is " + offsetrange.partition + " and the committed offset is " + offsetrange.untilOffset + " ================")
      }

*/


      println("check point testing good now")
    } catch{
      case e: Exception => {e.printStackTrace; println("exception is caught")}
    } finally{
      TestKafkaServer.stop()


    }

  }


}
