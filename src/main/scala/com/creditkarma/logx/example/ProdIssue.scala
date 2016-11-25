package com.creditkarma.logx.example

/**
  * Created by shengwei.wang on 11/22/16.
  */
import java.io.{ByteArrayInputStream, SequenceInputStream}

import com.creditkarma.logx.utils.gcs.{GCSUtils}
import com.google.api.client.http.InputStreamContent
import com.google.api.services.storage.model.StorageObject
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import java.io._

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import com.google.api.services.storage.Storage

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.spark.streaming.kafka010.{KafkaUtils, OffsetRange}
import org.apache.kafka.common.TopicPartition
import com.google.api.client.http.HttpTransport
import com.google.api.client.json.JsonFactory
import com.google.api.services.storage.model.StorageObject
import com.google.api.client.http.InputStreamContent
import java.io._;


object ProdIssue {

  def main(args: Array[String]) {

    // Create a Scala Spark Context.
    val conf = new SparkConf().setAppName("spark gcs connector test").setMaster("local[8]")
    val sc = new SparkContext(conf)

    sc.hadoopConfiguration.set("fs.gs.project.id", "295779567055")
    sc.hadoopConfiguration.set("fs.gs.impl","com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    sc.hadoopConfiguration.set("google.cloud.auth.service.account.enable", "true")
    sc.hadoopConfiguration.set("google.cloud.auth.service.account.email", "dataeng-test@modular-shard-92519.iam.gserviceaccount.com")
    sc.hadoopConfiguration.set("google.cloud.auth.service.account.keyfile", "/Users/shengwei.wang/projects/DataScience-ac040bae47fb.p12")


    // this is a read test
    //Buckets/ck30616327/new/insert/kafka/sponge_OfferVisible/2016/11/20
    //Buckets/ck30616327/new/insert/kafka/sponge_OfferVisible/2016/11/21
    val myRDD = sc.textFile("gs://ck30616327/new/insert/kafka/sponge_OfferVisible/2016/11/21/daily_composed_1479728392374_AqVyAz_50.json")
    //myRDD.take(100).foreach(myString => println("-----------"+myString+"--------"))

myRDD.foreach(tempString =>{
if(tempString.contains("loanRate")) {
  //println(" ============= > before: " + tempString)

  val after:String =tempString.replaceAll(
    "(\"loanRate\":)([0-9.]+)(,)","$1"+ "\"" +"$2"+"\"" +","

  )

  //println(" ============= > after: " + after)
}
})

    val newRDD = myRDD.map(tempString =>{

     // println(" ============= > all: " + tempString)
      var after:String = tempString

      if(tempString.contains("loanRate")) {
       // println(" ============= > before: " + tempString)

        after =tempString.replaceAll(
          "(\"loanRate\":)([0-9.]+)(,)","$1"+ "\"" +"$2"+"\"" +","

        )

        //println(" ============= > after: " + after)
      }

      after
    })

    //myRDD.take(100).foreach(tempString => println(tempString))
    //newRDD.take(1).foreach(tempString => println(tempString))
    newRDD.foreach(afterString => {
      if(afterString.contains("loanRate"))
      println(afterString)})
/*
    val words = myRDD.flatMap(line => line.split(" "))
    // Transform into pairs and count.
    val counts = words.map(word => (word, 1)).reduceByKey{case (x, y) => x + y} // Save the word count back out to a text file, causing evaluation. counts.saveAsTextFile(outputFile)
    counts.collect().foreach(println)
*/
    // this is a write test
   // myRDD.saveAsTextFile("gs://dataeng_test/prodissue")

   // newRDD.saveAsObjectFile("/Users/shengwei.wang/Desktop/crap.json")
    //newRDD.saveAsObjectFile("gs://dataeng_test/prodissue_temp/crap.json")







    ////////



        val storage = GCSUtils.getService("/Users/shengwei.wang/projects/DataScience-f7d364638ad4.json", 10000, 10000)

        val request = storage.objects.insert(
          "dataeng_test",
          new StorageObject().setName("fromkafkatest_clientapi/usingClientAPI" + "bigbigbig.json"),
          new InputStreamContent("application/json", iteratorToStream(newRDD.toLocalIterator) // don't forget add new line for each string
        )
        )
        request.getMediaHttpUploader.setDirectUploadEnabled(true)
        request.execute()



    /////



  }


  def iteratorToStream (strings: Iterator[String]): InputStream = {
    new SequenceInputStream({
      val i = strings map { s => new ByteArrayInputStream(s.getBytes("UTF-8")) }
      i.asJavaEnumeration
    })
  }






}