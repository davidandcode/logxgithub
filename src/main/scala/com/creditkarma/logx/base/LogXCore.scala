package com.creditkarma.logx.base

import com.creditkarma.logx.instrumentation._
import com.creditkarma.logx.utils.LazyLog

import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

/**
  * Created by yongjia.wang on 11/16/16.
  */

class LogXCore[S <: Source, K <: Sink, I <: StreamData, O <: StreamData, C <: Checkpoint]
(
  val appName: String,
  reader: StreamReader[S, I, C],
  transformer: Transformer[I, O],
  writer: Writer[K, O, C],
  checkPointService: CheckpointService[C]
) extends Module with Instrumentable {

  override def moduleType: ModuleType.Value = ModuleType.Core
  override def registerInstrumentor(instrumentor: Instrumentor): Unit = {
    super.registerInstrumentor(instrumentor)
    reader.registerInstrumentor(instrumentor)
    transformer.registerInstrumentor(instrumentor)
    writer.registerInstrumentor(instrumentor)
    checkPointService.registerInstrumentor(instrumentor)
  }

  def runOneCycle(cycleId: Long) = {

    instrumentors.foreach(_.cycleStarted)

    /**
      * None of the modules should handle any exception, but propagate the exception messages back here as part of instrumentation
      */
    Try(checkPointService.lastCheckpoint())
    match {
      case Success(lastCheckpoint) =>
        instrumentors.foreach(_.updateStatus(checkPointService, new StatusOK(s"Got last checkpoint ${lastCheckpoint}")))
        Try(reader.fetchData(lastCheckpoint))
        match {
          case Success(nextCheckpoint) =>
            instrumentors.foreach(_.updateStatus(reader, new StatusOK(s"Fetched records ${reader.fetchedRecords}")))
            writer.readCheckpoint = Some(nextCheckpoint)
            if (reader.flushDownstream()) {
              reader.lastFlushTime = System.currentTimeMillis()
              reader.flushId = reader.flushId + 1
              writer.flushId = reader.flushId
              instrumentors.foreach(_.updateStatus(reader, new StatusOK("ready to flush")))
              Try(transformer.transform(reader.fetchedData))
              match {
                case Success(data) =>
                  Try(writer.write(data)) match {
                    case Success(writerCheckpoint) =>
                      instrumentors.foreach(_.updateStatus(writer, new StatusOK("ready to checkpoint")))

                      Try(checkPointService.commitCheckpoint(writerCheckpoint))
                      match {
                        case Success(_) =>
                          instrumentors.foreach(_.updateStatus(checkPointService,
                            new StatusOK("checkpoint success")))
                        case Failure(f) =>
                          instrumentors.foreach(_.updateStatus(checkPointService, new StatusError(f)))
                      }

                    case Failure(f)=>
                      instrumentors.foreach(_.updateStatus(writer, new StatusError(f)))
                  }
                case Failure(f) =>
                  instrumentors.foreach(_.updateStatus(transformer, new StatusError(f)))
              }
            }
            else {
              // not enough to flush downstream, just do nothing and wait for next cycle
              instrumentors.foreach(_.updateStatus(reader, new StatusOK("not enough to flush")))
            }
          case Failure(f) =>
            instrumentors.foreach(_.updateStatus(reader, new StatusError(f)))
        }
      case Failure(f) =>
        instrumentors.foreach(_.updateStatus(checkPointService, new StatusError(f)))

    }
    instrumentors.foreach(_.cycleCompleted)
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
    instrumentors.foreach(_.updateStatus(this, new StatusOK("Shutting down")))
    reader.close()
    writer.close()
  }
}
