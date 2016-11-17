package com.creditkarma.logx.core

import com.creditkarma.logx.instrumentation.Instrumentable

/**
  *
  * @tparam S source of the reader
  * @tparam D type of the input payloads, which will go to transformer and eventually writer
  */
trait StreamReader[S <: Source, D <: StreamData, C <: Checkpoint] extends Module with Instrumentable {
  val source: S
  override def moduleType: ModuleType.Value = ModuleType.Reader

  def start(): Boolean
  def close(): Unit

  /**
    * Fetch data from checkpoint all the way to the head of the stream
    * In case of back filling with a big time window, the data may be very large, it's the writer's responsibility to properly write them
    * Certain complicated transformation (involving aggregation) may also require prohibitive resources for large inputs
    * Depending on the implementation, this method can potentially fetch data into buffer until it meets the flush condition
    * When using lazy read such as in Spark, there is no need to deal with buffering at read time, but only about meta data
    * @param checkpoint
    * @return the next checkpoint if the fetched data are successfully written
    *
    */
  def fetchData(checkpoint: C): C

  /**
    * This is about streaming flush policy, can be based on data size, time interval or combination
    * @return whether the currently fetched data should be send down the stream
    */
  def flushDownstream(): Boolean

  def fetchedRecords: Long

  def fetchedData: D

  // The flush policy may look at this to make sure streaming interval is no more than the threshold
  var lastFlushTime: Long = 0

  var flushId: Long = 0

}