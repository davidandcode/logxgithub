package com.creditkarma.logx.core

/**
  * Created by yongjia.wang on 11/16/16.
  */
trait Transformer [I <: StreamData, O <: StreamData] extends Module {
  def transform(input: I): O
  override def moduleType: ModuleType.Value = ModuleType.Transformer
}