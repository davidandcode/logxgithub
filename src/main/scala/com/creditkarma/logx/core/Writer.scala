package com.creditkarma.logx.core

import com.creditkarma.logx.impl.checkpoint.KafkaCheckpoint
import com.creditkarma.logx.instrumentation.Instrumentable

/**
  * Created by yongjia.wang on 11/16/16.
  */
trait Writer [S <: Sink, D <: StreamData, C <: Checkpoint] extends Module  with Instrumentable {
  val sink: S
  def start(): Boolean
  def close(): Unit
  def write(data: D): C
  override def moduleType: ModuleType.Value = ModuleType.Writer
  var flushId: Long = 0
  var readCheckpoint: Option[C] = None
}
