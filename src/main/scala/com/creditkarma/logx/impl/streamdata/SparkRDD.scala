package com.creditkarma.logx.impl.streamdata

import com.creditkarma.logx.core.StreamData
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD

/**
  * Created by yongjia.wang on 11/16/16.
  */
class SparkRDD[T](val rdd: RDD[T]) extends StreamData {}
