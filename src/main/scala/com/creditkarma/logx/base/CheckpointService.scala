package com.creditkarma.logx.base

/**
  * Created by yongjia.wang on 11/16/16.
  */
trait CheckpointService [C <: Checkpoint[_ , _]] extends Module with Instrumentable {
  def commitCheckpoint(cp: C): Unit
  def lastCheckpoint(): C

  override def moduleType: ModuleType.Value = ModuleType.CheckpointService
}
