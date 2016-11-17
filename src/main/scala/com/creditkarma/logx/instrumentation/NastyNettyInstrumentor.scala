package com.creditkarma.logx.instrumentation
import com.creditkarma.logx.core.Module
import com.creditkarma.logx.utils.LazyLog
import org.apache.log4j.{ConsoleAppender, Level, PatternLayout}

/**
  * Created by yongjia.wang on 11/17/16.
  */
object NastyNettyInstrumentor extends Instrumentor with LazyLog {

  override def name: String = "Nasty Netty"

  setLevel(Level.DEBUG)
  printLogToConsole()

  override def cycleStarted(cycleId: Long): Unit = {
    info(s"Cycle $cycleId started")
  }

  override def cycleCompleted(cycleId: Long): Unit = {
    info(s"Cycle $cycleId completed")
  }

  override def updateStatus(cycleId: Long, module: Module, status: Status): Unit = {
    info(s"Cycle=$cycleId, Module=${module.getClass.getCanonicalName} type=${module.moduleType}, status=${status}")
  }

  override def updateMetricLong(cycleId: Long, module: Module, metricType: MetricType.Value, metricValue: Long,
                                args: Map[MetricArgs.Value, Any]): Unit = {
    info(s"Cycle=$cycleId, Module=${module.getClass.getCanonicalName} type=${module.moduleType}, $metricType=$metricValue, $args=$args")
  }

  override def updateMetricDouble(cycleId: Long, module: Module, metricType: MetricType.Value, metricValue: Double,
                                  args: Map[MetricArgs.Value, Any]): Unit = {
    info(s"Cycle=$cycleId, Module=${module.getClass.getCanonicalName} type=${module.moduleType}, $metricType=$metricValue, $args=$args")
  }
}
