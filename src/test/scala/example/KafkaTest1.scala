package example

import java.io.{BufferedReader, FileReader}

import com.creditkarma.logx.base.{Checkpoint, CheckpointService, Sink, Source, StreamBuffer, StreamReader, Transformer, Writer, _}
import com.creditkarma.logx.impl.checkpoint.KafkaCheckpoint
import com.creditkarma.logx.impl.sourcesink.Kafka
import com.creditkarma.logx.impl.streambuffer.SparkRDD
import com.creditkarma.logx.impl.streamreader.KafkaSparkRDDReader
import com.creditkarma.logx.instrumentation.LogInfoInstrumentor
import info.batey.kafka.unit.KafkaUnit
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

/**
  * Created by yongjia.wang on 11/17/16.
  */
object KafkaTest1 {

  class IdentityTransformer[I <: StreamBuffer] extends Transformer[I, I]{
    override def transform(input: I): I = input
  }

  class DummySink extends Sink{
    override def name: String = "dummy sink"
  }

  class KafkaSparkRDDMessageCollector(collectedData: ListBuffer[String]) extends Writer[DummySink, SparkRDD[ConsumerRecord[String, String]], KafkaCheckpoint]{
    override val sink = null

    override def write(data: SparkRDD[ConsumerRecord[String, String]]): KafkaCheckpoint = {
      collectedData ++= data.rdd.map(_.value()).collect()
      nextCheckpoint.get
    }
  }

  class InMemoryKafkaCheckpointService extends CheckpointService[KafkaCheckpoint]{
    val lastCheckPoint: KafkaCheckpoint = new KafkaCheckpoint()
    override def commitCheckpoint(cp: KafkaCheckpoint): Unit = {
      lastCheckPoint.mergeKafkaCheckpoint(cp)
    }

    override def lastCheckpoint(): KafkaCheckpoint = {
      lastCheckPoint
    }
  }

  object TestKafkaServer {

    val kafkaUnitServer = new KafkaUnit(5556, 5558)
    val kp = new KafkaProducer[String, String](kafkaParams.asJava)

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
        if(line != null){
          testData.append(line)
          true
        }
        else{
          false
        }
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
    "bootstrap.servers" -> "localhost:5558",
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



    val collectedData: ListBuffer[String] = ListBuffer.empty
    val testLogX = new LogXCore(
      "test-logX",
      new KafkaSparkRDDReader[String, String](new Kafka(kafkaParams)).setMatchFetchRecords(1),
      new IdentityTransformer[SparkRDD[ConsumerRecord[String, String]]](),
      new KafkaSparkRDDMessageCollector(collectedData),
      new InMemoryKafkaCheckpointService()
    )


    testLogX.registerInstrumentor(LogInfoInstrumentor)

    TestKafkaServer.sendNextMessage(5)
    testLogX.runOneCycle()
    println(collectedData)
    TestKafkaServer.sendNextMessage(4)
    testLogX.runOneCycle()
    println(collectedData)
    TestKafkaServer.sendNextMessage(4)
    testLogX.runOneCycle()
    println(collectedData)

    println(collectedData.size)
    println(TestKafkaServer.sentMessages.size)
    assert(collectedData.toSet == TestKafkaServer.sentMessages.toSet && collectedData.size==TestKafkaServer.sentMessages.size)

    testLogX.close()

    TestKafkaServer.stop()
  }

  def loadDataToLocalKafka(kafkaUnitServer: KafkaUnit) = {
    kafkaUnitServer.startup()

    val kp = new KafkaProducer[String, String](kafkaParams.asJava)
    val br = new BufferedReader(new FileReader("test_data/KRSOffer.json"))
    while({
      val line = br.readLine()
      kp.send(new ProducerRecord[String, String]("testTopic", null, line))
      kp.flush()
      line != null
    }){
    }

    kp.close()
  }
}
