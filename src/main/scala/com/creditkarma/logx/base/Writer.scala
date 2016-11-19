package com.creditkarma.logx.base

import com.creditkarma.logx.impl.checkpoint.KafkaCheckpoint

/**
  * Writer flushes stream buffer into the sink
  */
trait Writer [B <: BufferedData, C <: Checkpoint[D, C], D] extends Module  with Instrumentable {
  def start(): Unit = {}
  def close(): Unit = {}

  /**
    *
    * @param data Data in the buffer to be flushed
    * @param delta
    * @return The delta successfully written for the purpose of checkpoint. If all data are written, it's the same as delta
    */
  def write(data: B, delta: D): D
  override def moduleType: ModuleType.Value = ModuleType.Writer
  var flushId: Long = 0

}
