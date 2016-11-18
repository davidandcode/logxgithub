package com.creditkarma.logx.base

/**
  * Created by yongjia.wang on 11/16/16.
  */
object MetricArgs extends Enumeration{
  val
  KafkaTopic,
  ReaderDataCount, ReaderDataBytes,
  WriterDataCount, WriterDataBytes,
  FlushedDataCount, FlushedDataBytes,
  CheckpointBytes = Value
}
