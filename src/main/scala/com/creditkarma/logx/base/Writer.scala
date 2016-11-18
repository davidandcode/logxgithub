package com.creditkarma.logx.base

import com.creditkarma.logx.impl.checkpoint.KafkaCheckpoint

/**
  * Writer flushes stream buffer into the sink
  */
trait Writer [S <: Sink, D <: StreamBuffer, C <: Checkpoint] extends Module  with Instrumentable {
  val sink: S
  def start(): Boolean
  def close(): Unit

  /**
    *
    * @param data Data in the buffer to be flushed
    * @return The checkpoint of the actually written data.
    *         In the simplest case, it's the [[nextCheckpoint]] if write is successful, and [[previousCheckpoint]] otherwise
    *         In some cases, buffer can be partial written and the writer returned checkpoint will be somewhere in the middle
    */
  def write(data: D): C
  override def moduleType: ModuleType.Value = ModuleType.Writer
  var flushId: Long = 0

  /**
    * This is the checkpoint of the end of buffer, which is set by the reader
    */
  var nextCheckpoint: Option[C] = None
  /**
    * This is the checkpoint of the beginning of buffer, which is set by the reader
    */
  var previousCheckpoint: Option[C] = None
}
