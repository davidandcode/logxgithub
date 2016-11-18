package com.creditkarma.logx.impl.streambuffer

import com.creditkarma.logx.base.StreamBuffer
import org.apache.spark.sql.Dataset

/**
  * Created by yongjia.wang on 11/16/16.
  */
class SparkDataset[T] (val dataSet: Dataset[T]) extends StreamBuffer {}
