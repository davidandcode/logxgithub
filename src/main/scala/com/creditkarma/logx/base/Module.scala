package com.creditkarma.logx.base

/**
  * Created by yongjia.wang on 11/16/16.
  */
trait Module {
  def moduleType: ModuleType.Value
}


object ModuleType extends Enumeration {
  val Core, Reader, Writer, Transformer, CheckpointService = Value
}