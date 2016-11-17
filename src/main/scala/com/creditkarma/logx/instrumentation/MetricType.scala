package com.creditkarma.logx.instrumentation

/**
  * Created by yongjia.wang on 11/16/16.
  */
object MetricType extends Enumeration {
  val
  ReaderDataCount, ReaderDataBytes,
  WriterDataCount, WriterDataBytes,
  FlushedDataCount, FlushedDataBytes,
  CheckpointBytes
  = Value
}
