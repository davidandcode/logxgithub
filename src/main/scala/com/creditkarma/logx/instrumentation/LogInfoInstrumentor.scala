package com.creditkarma.logx.instrumentation
import com.creditkarma.logx.base.{Instrumentor, MetricArgs, Module, Status}
import com.creditkarma.logx.utils.LazyLog
import org.apache.log4j.{ConsoleAppender, Level, LogManager, PatternLayout}

import scala.collection.JavaConverters._
/**
  * Created by yongjia.wang on 11/17/16.
  */
object LogInfoInstrumentor extends Instrumentor with LazyLog {

  def printLogToConsole(): Unit = {
    val appenders = LogManager.getRootLogger.getAllAppenders.asScala
    if(!appenders.exists(_.isInstanceOf[ConsoleAppender])){ // if the appender is already at the root, which Spark does by default, don't add it
      val appender = new ConsoleAppender(new PatternLayout("%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n"))
      logger.addAppender(appender)
    }
  }
  def setLevel(level: Level): Unit = {
    logger.setLevel(level)
    this
  }

  setLevel(Level.INFO)
  printLogToConsole()

  override def name: String = this.getClass.getName

  var cycleId: Long = 0
  override def cycleStarted(): Unit = {
    info(s"Cycle $cycleId started")
  }

  override def cycleCompleted(): Unit = {
    info(s"Cycle $cycleId completed")
    cycleId += 1
  }

  override def updateStatus(module: Module, status: Status): Unit = {
    info(s"Cycle=$cycleId, Module=${module.getClass.getSimpleName}(type=${module.moduleType}), status=${status}")
  }

  override def updateMetric(module: Module, args: Map[MetricArgs.Value, Any]): Unit = {
    info(s"Cycle=$cycleId, Module=${module.getClass.getSimpleName}(type=${module.moduleType}), $args=$args")
  }
}
