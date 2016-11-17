package com.creditkarma.logx.core

/**
  * Created by yongjia.wang on 11/16/16.
  */
trait Writer [S <: Sink, D <: StreamData, C <: Checkpoint] extends Module{
  val sink: S
  def start(): Boolean
  def close(): Unit
  def write(data: D): C
  def writtenRecords: Long
  override def moduleType: ModuleType.Value = ModuleType.Writer
}
