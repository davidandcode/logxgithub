package com.creditkarma.logx.base

/**
  * Created by yongjia.wang on 11/17/16.
  */
trait Instrumentable {

  private val _instrumentors: scala.collection.mutable.Map[String, Instrumentor] = scala.collection.mutable.Map.empty

  private[base] def instrumentors = _instrumentors.values

  def registerInstrumentor(instrumentor: Instrumentor): Unit = {
    _instrumentors.get(instrumentor.name) match {
      case Some(ins) =>
        throw new Exception(
          s"Instrumentor with the same name already registered: ${instrumentor}\n" +
            s"Existing: ${ins.getClass.getCanonicalName}\n" +
            s"New: ${instrumentor.getClass.getCanonicalName}")
      case None =>
        _instrumentors += instrumentor.name -> instrumentor
    }
  }

  def statusUpdate(module: Module, status: Status): Unit = {
    instrumentors.foreach(_.updateStatus(module, status))
  }

  def metricUpdate(module: Module, metrics: Map[MetricArgs.Value, Any]): Unit = {
    instrumentors.foreach(_.updateMetric(module, metrics))
  }

  def cycleStarted(): Unit = {
    instrumentors.foreach(_.cycleStarted)
  }

  def cycleCompleted(): Unit = {
    instrumentors.foreach(_.cycleCompleted())
  }
}
