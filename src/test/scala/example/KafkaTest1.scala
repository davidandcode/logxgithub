package example

import java.io.{BufferedReader, FileReader}

import com.creditkarma.logx.core.{Checkpoint, CheckpointService, Sink, Source, StreamData, StreamReader, Transformer, Writer, _}
import com.creditkarma.logx.impl.checkpoint.KafkaCheckpoint
import com.creditkarma.logx.impl.sourcesink.Kafka
import com.creditkarma.logx.impl.streamdata.SparkRDD
import com.creditkarma.logx.impl.streamreader.KafkaSparkRDDReader
import com.creditkarma.logx.instrumentation.NastyNettyInstrumentor
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

  class IdentityTransformer[I <: StreamData] extends Transformer[I, I]{
    override def transform(input: I): I = input
  }

  class DummySink extends Sink{
    override def name: String = "dummy sink"
  }

  class KafkaSparkRDDMessageCollector extends Writer[DummySink, SparkRDD[ConsumerRecord[String, String]], KafkaCheckpoint]{
    override val sink = null

    override def start(): Boolean = true

    override def close(): Unit = {}

    override def write(data: SparkRDD[ConsumerRecord[String, String]]): KafkaCheckpoint = {
      collectedData = data.rdd.map(_.value()).collect()
      readCheckpoint.get
    }

    var collectedData: Seq[String] = Seq.empty
  }

  class InMemoryKafkaCheckpointService extends CheckpointService[KafkaCheckpoint]{
    var lastCheckPoint: KafkaCheckpoint = null
    override def commitCheckpoint(cp: KafkaCheckpoint): Unit = {
      lastCheckPoint = cp
    }

    override def lastCheckpoint(): KafkaCheckpoint = {
      if(lastCheckPoint == null){
        new KafkaCheckpoint()
      }
      else{
        lastCheckPoint
      }
    }
  }

  object TestKafkaServer {

    val kafkaUnitServer = new KafkaUnit(5556, 5558)
    val kp = new KafkaProducer[String, String](kafkaParams.asJava)

    def start(): Unit = {
      kafkaUnitServer.startup()
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



    val testLogX = new LogX[
      Kafka, DummySink, SparkRDD[ConsumerRecord[String, String]], SparkRDD[ConsumerRecord[String, String]], KafkaCheckpoint,
      KafkaSparkRDDReader[String, String], IdentityTransformer[SparkRDD[ConsumerRecord[String, String]]],
      KafkaSparkRDDMessageCollector, InMemoryKafkaCheckpointService
    ](
      "test-logX",
      new KafkaSparkRDDReader[String, String](new Kafka(kafkaParams)).setMatchFetchRecords(1),
      new IdentityTransformer[SparkRDD[ConsumerRecord[String, String]]](),
      new KafkaSparkRDDMessageCollector(),
      new InMemoryKafkaCheckpointService()
    )


    testLogX.registerInstrumentor(NastyNettyInstrumentor)

    TestKafkaServer.sendNextMessage()
    testLogX.runOneCycle(0)
    println(testLogX.writer.collectedData)
    TestKafkaServer.sendNextMessage()
    testLogX.runOneCycle(1)
    println(testLogX.writer.collectedData)

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
