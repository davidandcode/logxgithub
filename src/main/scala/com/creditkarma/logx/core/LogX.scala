package com.creditkarma.logx.core

import com.creditkarma.logx.instrumentation.{MetricReporter, Status, StatusCode}
import com.creditkarma.logx.utils.LazyLog

/**
  * Created by yongjia.wang on 11/16/16.
  */

abstract class LogX[S <: Source, K <: Sink, I <: StreamData, O <: StreamData, C <: Checkpoint]
(
  appName: String,
  reader: StreamReader[S, I, C],
  transformer: Transformer[I, O],
  writer: Writer[K, O, C],
  checkPointService: CheckpointService[C],
  metricReporter: MetricReporter
) extends LazyLog {

  def runOneCycle(cycleId: Long) = {
    metricReporter.cycleStarted(cycleId)
    val (readSuccess: Boolean, nextCheckPoint: C) =
      reader.fetchData(
        checkPointService.lastCheckpoint()
      )

    if(! readSuccess){
      metricReporter.updateStatus(cycleId, reader, new Status(StatusCode.FAILURE, ""))
    }
    else if (reader.flushDownstream()) {
      reader.lastFlushTime = System.currentTimeMillis()
      val writerCheckpoint = writer.write(
        transformer.transform(reader.fetchedData)
      )
      checkPointService.commitCheckpoint(writerCheckpoint)
    }
    else {
      // not enough to flush downstream, just do nothing and wait for next cycle
    }
    metricReporter.cycleCompleted(cycleId)
  }

  def start(pollingInterVal: Long = 1000): Unit = {
    if(reader.start() && writer.start()){
      scala.sys.addShutdownHook(close)
      var cycleId: Long = 0
      while(true){
        runOneCycle(cycleId)
        cycleId += 1
        Thread.sleep(pollingInterVal)
      }
    }
  }



  def close(): Unit = {
    reader.close()
    writer.close()
  }
}
