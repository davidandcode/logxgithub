package com.creditkarma.logx.core

/**
  * Created by yongjia.wang on 11/16/16.
  */
trait CheckpointService [C <: Checkpoint] extends Module{
  def commitCheckpoint(cp: C): Boolean
  def lastCheckpoint(): C

  override def moduleType: ModuleType.Value = ModuleType.CheckpointService
}
