package com.creditkarma.logx.base

import scala.util.{Failure, Success, Try}

/**
  * Created by yongjia.wang on 11/16/16.
  */

class LogXCore[I <: BufferedData, O <: BufferedData, C <: Checkpoint[Delta, C], Delta]
(
  val appName: String,
  reader: Reader[I, C, Delta, _],
  transformer: Transformer[I, O],
  writer: Writer[O, Delta, _],
  checkPointService: CheckpointService[C]
) extends Module {

  override def moduleType: ModuleType.Value = ModuleType.Core
  override def registerInstrumentor(instrumentor: Instrumentor): Unit = {
    super.registerInstrumentor(instrumentor)
    reader.registerInstrumentor(instrumentor)
    transformer.registerInstrumentor(instrumentor)
    writer.registerInstrumentor(instrumentor)
    checkPointService.registerInstrumentor(instrumentor)
  }

  var lastFlushTime: Long = 0
  def runOneCycle() = {
    instrumentors.foreach(_.cycleStarted)
    /**
      * None of the module implementations should swallow exception
      */
    // 1. Load checkpoint
    Try(checkPointService.executeLoad())
    match {
      // 1a. Load checkpoint success
      case Success(lastCheckpoint) =>
        statusUpdate(checkPointService, new StatusOK(s"Got last checkpoint ${lastCheckpoint}"))
        // 2. Read
        Try(reader.execute(lastFlushTime, lastCheckpoint))
        match {
          // 2a. Read success
          case Success((inData, inDelta, flush)) =>
            // 2a-1. Flush
            if (flush) {
              lastFlushTime = System.currentTimeMillis()
              statusUpdate(reader, new StatusOK("ready to flush"))
              // 3 transform
              Try(transformer.execute(inData))
              match {
                // 3a. Transform success
                case Success(outData) =>
                  // 4. Write
                  Try(writer.execute(outData)) match {
                    // 4a. Write success
                    case Success(outDelta) =>
                      statusUpdate(writer, new StatusOK("ready to checkpoint"))
                      // 5. Checkpoint
                      Try(
                        checkPointService.executeCommit(
                        lastCheckpoint
                          .mergeDelta(outDelta.getOrElse(inDelta)))
                      )
                      match {
                        // 5a. Checkpoint success
                        case Success(_) =>
                          statusUpdate(checkPointService, new StatusOK("checkpoint success"))
                        // 5b. Checkpoint failure
                        case Failure(f) =>
                          statusUpdate(checkPointService, new StatusError(f))
                      }
                    // 4b. Write failure
                    case Failure(f)=>
                      statusUpdate(writer, new StatusError(f))
                  }
                // 3b. Transform failure
                case Failure(f) =>
                  statusUpdate(transformer, new StatusError(f))
              }
            }
            // 2a-b. Not flush
            else {
              // not enough to flush downstream, just do nothing and wait for next cycle
              statusUpdate(reader, new StatusOK("not enough to flush"))
            }
          //2b. Read Failure
          case Failure(f) =>
            statusUpdate(reader, new StatusError(f))
        }
      //1b. load checkpoint failure
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
