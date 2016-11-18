package com.creditkarma.logx.base

/**
  * Created by yongjia.wang on 11/16/16.
  */
trait Transformer [I <: StreamData, O <: StreamData] extends Module with Instrumentable {
  def transform(input: I): O
  override def moduleType: ModuleType.Value = ModuleType.Transformer
}