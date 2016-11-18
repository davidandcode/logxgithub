package com.creditkarma.logx.impl.streamdata

import com.creditkarma.logx.base.StreamData
import org.apache.spark.sql.Dataset

/**
  * Created by yongjia.wang on 11/16/16.
  */
class SparkDataset[T] (dataSet: Dataset[T]) extends StreamData {

}
