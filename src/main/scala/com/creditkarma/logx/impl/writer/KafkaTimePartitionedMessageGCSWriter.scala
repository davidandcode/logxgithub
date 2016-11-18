package com.creditkarma.logx.impl.writer

import com.creditkarma.logx.base.Writer
import com.creditkarma.logx.impl.checkpoint.KafkaCheckpoint
import com.creditkarma.logx.impl.sourcesink.GoogleCloudStorage
import com.creditkarma.logx.impl.streambuffer.SparkRDD
import com.creditkarma.logx.impl.transformer.KafkaTimePartitionedMessage

/**
  * Created by yongjia.wang on 11/18/16.
  */
class KafkaTimePartitionedMessageGCSWriter(val sink: GoogleCloudStorage)
  extends Writer [GoogleCloudStorage, SparkRDD[KafkaTimePartitionedMessage], KafkaCheckpoint]{

  //re-use gcs client object if possible

  override def write(data: SparkRDD[KafkaTimePartitionedMessage]): KafkaCheckpoint = nextCheckpoint.getOrElse(new KafkaCheckpoint())
}
