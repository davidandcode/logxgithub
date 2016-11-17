package com.creditkarma.logx.core

import com.creditkarma.logx.instrumentation.{Instrumentable, Instrumentor, Status, StatusCode}
import com.creditkarma.logx.utils.LazyLog

import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

/**
  * Created by yongjia.wang on 11/16/16.
  */

class LogX[
S <: Source, K <: Sink, I <: StreamData, O <: StreamData, C <: Checkpoint,
R <: StreamReader[S, I, C], T <: Transformer[I, O], W <: Writer[K, O, C], CS <: CheckpointService[C]
]
(
  val appName: String,
  val reader: R,
  val transformer: T,
  val writer: W,
  val checkPointService: CS
) extends Instrumentable {

  override def registerInstrumentor(instrumentor: Instrumentor): Boolean = {
    super.registerInstrumentor(instrumentor) &&
    reader.registerInstrumentor(instrumentor) &&
      transformer.registerInstrumentor(instrumentor) &&
      writer.registerInstrumentor(instrumentor) &&
      checkPointService.registerInstrumentor(instrumentor)
  }

  def runOneCycle(cycleId: Long) = {

    reader.cycleId = cycleId
    writer.cycleId = cycleId
    transformer.cycleId = cycleId
    checkPointService.cycleId = cycleId

    instrumentors.foreach(_.cycleStarted(cycleId))

    /**
      * None of the modules should handle any exception, but propagate the exception messages back here as part of instrumentation
      */
    Try(checkPointService.lastCheckpoint())
    match {
      case Success(lastCheckpoint) =>
        instrumentors.foreach(_.updateStatus(cycleId, checkPointService,
          new Status(StatusCode.SUCCESS, s"Got last checkpoint ${lastCheckpoint}")))
        Try(reader.fetchData(lastCheckpoint))
        match {
          case Success(nextCheckpoint) =>
            instrumentors.foreach(_.updateStatus(cycleId, reader,
              new Status(StatusCode.SUCCESS, s"Fetched records ${reader.fetchedRecords}")))
            writer.readCheckpoint = Some(nextCheckpoint)
            if (reader.flushDownstream()) {
              reader.lastFlushTime = System.currentTimeMillis()
              reader.flushId = reader.flushId + 1
              writer.flushId = reader.flushId
              instrumentors.foreach(_.updateStatus(cycleId, reader, new Status(StatusCode.SUCCESS, "ready to flush")))
              Try(transformer.transform(reader.fetchedData))
              match {
                case Success(data) =>
                  Try(writer.write(data)) match {
                    case Success(writerCheckpoint) =>
                      instrumentors.foreach(_.updateStatus(cycleId, writer, new Status(StatusCode.SUCCESS, "ready to checkpoint")))

                      Try(checkPointService.commitCheckpoint(writerCheckpoint))
                      match {
                        case Success(_) =>
                          instrumentors.foreach(_.updateStatus(cycleId, checkPointService,
                            new Status(StatusCode.SUCCESS, "checkpoint success")))
                        case Failure(f) =>
                          instrumentors.foreach(_.updateStatus(cycleId, checkPointService,
                            new Status(StatusCode.FAILURE, f.getMessage)))
                      }

                    case Failure(f)=>
                      instrumentors.foreach(_.updateStatus(cycleId, writer, new Status(StatusCode.FAILURE, f.getMessage)))
                  }
                case Failure(f) =>
                  instrumentors.foreach(_.updateStatus(cycleId, transformer, new Status(StatusCode.FAILURE, f.getMessage)))
              }
            }
            else {
              // not enough to flush downstream, just do nothing and wait for next cycle
              instrumentors.foreach(_.updateStatus(cycleId, reader, new Status(StatusCode.SUCCESS, "not enough to flush")))
            }
          case Failure(f) =>
            instrumentors.foreach(_.updateStatus(cycleId, reader, new Status(StatusCode.FAILURE, f.getMessage)))
        }
      case Failure(f) =>
        instrumentors.foreach(_.updateStatus(cycleId, checkPointService,
          new Status(StatusCode.FAILURE, f.getMessage)))

    }
    instrumentors.foreach(_.cycleCompleted(cycleId))
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
