package com.creditkarma.logx.instrumentation

import com.creditkarma.logx.core.Module

/**
  * The interface exposes LogX specific status and metrics at the event level
  * Implementations of the modules (reader, writer, etc.) are responsible to call the methods
  * Implementations of the metric reporters are responsible to interpret and process the metrics
  * It's important the metric reporter itself consumes little resource and all the methods should return immediately (no blocking IO etc.)
  */
trait Instrumentor {

  def name: String

  def cycleStarted(cycleId: Long): Unit

  def cycleCompleted(cycleId: Long): Unit

  def updateStatus(cycleId: Long, module: Module, status: Status): Unit

  def updateMetricLong(cycleId: Long, module: Module, metricType: MetricType.Value, metricValue: Long,
                       args: Map[MetricArgs.Value, Any] = Map.empty): Unit

  def updateMetricDouble(cycleId: Long, module: Module, metricType: MetricType.Value, metricValue: Double,
                         args: Map[MetricArgs.Value, Any] = Map.empty): Unit

}
