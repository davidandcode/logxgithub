package com.creditkarma.logx.instrumentation

/**
  * Created by yongjia.wang on 11/17/16.
  */
trait Instrumentable {

  private val _instrumentors: scala.collection.mutable.Map[String, Instrumentor] = scala.collection.mutable.Map.empty

  def instrumentors = _instrumentors.values

  def registerInstrumentor(instrumentor: Instrumentor): Boolean = {
    _instrumentors.get(instrumentor.name) match {
      case Some(ins) => false
      case None => _instrumentors += instrumentor.name -> instrumentor
        true
    }
  }
}
