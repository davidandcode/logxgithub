package com.creditkarma.logx.example

/**
  * Created by shengwei.wang on 11/21/16.
  */

import java.io.{BufferedReader, FileReader}

import com.creditkarma.logx.base.{BufferedData, CheckpointService, Transformer, Writer, _}
import com.creditkarma.logx.impl.checkpoint.KafkaCheckpoint
import com.creditkarma.logx.impl.checkpointservice.KafkaCheckpointService
import com.creditkarma.logx.impl.streambuffer.SparkRDD
import com.creditkarma.logx.impl.streamreader.KafkaSparkRDDReader
import com.creditkarma.logx.instrumentation.LogInfoInstrumentor
import info.batey.kafka.unit.KafkaUnit
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.streaming.kafka010.OffsetRange
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.zookeeper.ZooKeeper

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.collection.JavaConversions._

/**
  * Created by yongjia.wang on 11/17/16.
  */
object OldTest {

  class IdentityTransformer[I <: BufferedData] extends Transformer[I, I]{
    override def transform(input: I): I = input
  }


  val (zkPort, kafkaPort) = (5556, 5558)

  object TestKafkaServer {

    val kafkaUnitServer = new KafkaUnit(zkPort, kafkaPort)
    val kp = new KafkaProducer[String, String](kafkaParams)

    def start(): Unit = {
      kafkaUnitServer.startup()
      kafkaUnitServer.createTopic("testTopic", 5)
    }

    val testData: ListBuffer[String] = ListBuffer.empty
    var dataSentToKafka = 0

    def loadData (dataFile: String): Unit = {

      val br = new BufferedReader(new FileReader(dataFile))
      while({
        val line = br.readLine()
        kp.send(new ProducerRecord[String, String]("testTopic", null, line))
        kp.flush()
        line != null
      }){
      }

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

    try{

      TestKafkaServer.start()
      TestKafkaServer.loadData("test_data/KRSOffer.json")

      LogManager.getLogger("org.apache").setLevel(Level.WARN)
      LogManager.getLogger("kafka").setLevel(Level.WARN)


      SparkContext.getOrCreate(new SparkConf().setAppName("test").setMaster("local[2]")
        .set("spark.driver.host", "127.0.0.1")
        // set local host explicitly, the call through java.net.InetAddress.getLocalHost on laptop with VPN can be inconsistent
        // Also if it returns IPV6, Spark won't work with it
      )



     // val conn: ZooKeeperConnection = new ZooKeeperConnection();
     // val zk:ZooKeeper = conn.connect("localhost:" + zkPort);



    } catch{
      case e: Exception => e.printStackTrace
    } finally{
      TestKafkaServer.stop()
    }

  }

}