package com.creditkarma.logx.core

import com.creditkarma.logx.instrumentation.Instrumentable

/**
  * Created by yongjia.wang on 11/16/16.
  */
trait CheckpointService [C <: Checkpoint] extends Module with Instrumentable {
  def commitCheckpoint(cp: C): Unit
  def lastCheckpoint(): C

  override def moduleType: ModuleType.Value = ModuleType.CheckpointService
}
