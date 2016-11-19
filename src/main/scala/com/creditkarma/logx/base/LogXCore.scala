package com.creditkarma.logx.base

import scala.util.{Failure, Success, Try}

/**
  * Created by yongjia.wang on 11/16/16.
  */

class LogXCore[I <: BufferedData, O <: BufferedData, C <: Checkpoint[D, C], D]
(
  val appName: String,
  reader: Reader[I, C, D],
  transformer: Transformer[I, O],
  writer: Writer[O, C, D],
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

  def runOneCycle() = {

    instrumentors.foreach(_.cycleStarted)

    /**
      * None of the modules should handle any exception, but propagate the exception messages back here as part of instrumentation
      */
    Try(checkPointService.lastCheckpoint())
    match {
      case Success(lastCheckpoint) =>
        statusUpdate(checkPointService, new StatusOK(s"Got last checkpoint ${lastCheckpoint}"))
        Try(reader.fetchData(lastCheckpoint))
        match {
          case Success((fetchedData, delta)) =>
            statusUpdate(reader, new StatusOK(s"Fetched records ${reader.getNumberOfRecords(fetchedData, delta)}"))
            statusUpdate(reader, new StatusOK(s"Fetched Bytes ${reader.getBytes(fetchedData, delta)}"))
            if (reader.flushDownstream(fetchedData, delta)) {
              reader.lastFlushTime = System.currentTimeMillis()
              reader.flushId = reader.flushId + 1
              writer.flushId = reader.flushId
              statusUpdate(reader, new StatusOK("ready to flush"))
              Try(transformer.transform(fetchedData))
              match {
                case Success(dataToWrite) =>
                  Try(writer.write(dataToWrite, delta)) match {
                    case Success(writerDelta) =>
                      statusUpdate(writer, new StatusOK("ready to checkpoint"))
                      Try(checkPointService.commitCheckpoint(lastCheckpoint.mergeDelta(writerDelta)))
                      match {
                        case Success(_) =>
                          statusUpdate(checkPointService, new StatusOK("checkpoint success"))
                        case Failure(f) =>
                          statusUpdate(checkPointService, new StatusError(f))
                      }
                    case Failure(f)=>
                      statusUpdate(writer, new StatusError(f))
                  }
                case Failure(f) =>
                  statusUpdate(transformer, new StatusError(f))
              }
            }
            else {
              // not enough to flush downstream, just do nothing and wait for next cycle
              statusUpdate(reader, new StatusOK("not enough to flush"))
            }
          case Failure(f) =>
            statusUpdate(reader, new StatusError(f))
        }
      case Failure(f) =>
        statusUpdate(checkPointService, new StatusError(f))
    }
    instrumentors.foreach(_.cycleCompleted)
  }

  def start(pollingInterVal: Long = 1000): Unit = {

    Try(reader.start())
    match {
      case Success(_) =>
        Try(writer.start())
        match {
          case Success(_) =>

            scala.sys.addShutdownHook(close)

            while (true) {
              runOneCycle()
              Thread.sleep(pollingInterVal)
            }
          case Failure(f) => throw new Exception(s"Failed to start writer ${writer}", f)
        }
      case Failure(f) => throw new Exception(s"Failed to start reader ${reader}", f)
    }

  }


  def close(): Unit = {
    instrumentors.foreach(_.updateStatus(this, new StatusOK("Shutting down")))
    reader.close()
    writer.close()
  }
}
