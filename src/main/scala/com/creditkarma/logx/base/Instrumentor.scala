package com.creditkarma.logx.base

/**
  * The interface exposes LogX specific status and metrics at the event level
  * Implementations of the modules (reader, writer, etc.) are responsible to call the methods
  * Implementations of the metric reporters are responsible to interpret and process the metrics
  * It's important the metric reporter itself consumes little resource and all the methods should return immediately (no blocking IO etc.)
  */
trait Instrumentor {

  def name: String

  def cycleStarted(): Unit

  def cycleCompleted(): Unit

  def updateStatus(module: Module, status: Status): Unit

  def updateMetric(module: Module, args: Map[MetricArgs.Value, Any] = Map.empty): Unit

}
