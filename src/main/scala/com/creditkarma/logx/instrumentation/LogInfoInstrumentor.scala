package com.creditkarma.logx.instrumentation
import com.creditkarma.logx.base.{Instrumentor, MetricArgs, Module, Status}
import com.creditkarma.logx.utils.LazyLog
import org.apache.log4j.{ConsoleAppender, Level, PatternLayout}

/**
  * Created by yongjia.wang on 11/17/16.
  */
object LogInfoInstrumentor extends Instrumentor with LazyLog {

  override def name: String = this.getClass.getName

  setLevel(Level.DEBUG)
  printLogToConsole()

  var cycleId: Long = 0
  override def cycleStarted(): Unit = {
    info(s"Cycle $cycleId started")
  }

  override def cycleCompleted(): Unit = {
    info(s"Cycle $cycleId completed")
    cycleId += 1
  }

  override def updateStatus(module: Module, status: Status): Unit = {
    info(s"Cycle=$cycleId, Module=${module.getClass.getCanonicalName} type=${module.moduleType}, status=${status}")
  }

  override def updateMetric(module: Module, args: Map[MetricArgs.Value, Any]): Unit = {
    info(s"Cycle=$cycleId, Module=${module.getClass.getCanonicalName} type=${module.moduleType}, $args=$args")
  }
}
