package com.creditkarma.logx.impl.writer

import com.creditkarma.logx.base.Writer
import com.creditkarma.logx.impl.checkpoint.KafkaCheckpoint
import com.creditkarma.logx.impl.streambuffer.SparkRDD
import com.creditkarma.logx.impl.transformer.KafkaTimePartitionedMessage
import org.apache.spark.streaming.kafka010.OffsetRange

/**
  * Created by yongjia.wang on 11/18/16.
  */
class KafkaTimePartitionedMessageGCSWriter()
  extends Writer [SparkRDD[KafkaTimePartitionedMessage], KafkaCheckpoint, Seq[OffsetRange]]{

  //re-use gcs client object if possible

  override def write(data: SparkRDD[KafkaTimePartitionedMessage], delta: Seq[OffsetRange]): Seq[OffsetRange] = null

}
