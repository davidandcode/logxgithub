package com.creditkarma.logx.base

import scala.util.{Failure, Success, Try}

/**
  * Created by yongjia.wang on 11/16/16.
  *
  */


/**
  * @tparam Meta meta data of the transformation, used to construct checkpoint delta and metrics
  */
trait Transformer[I <: BufferedData, O <: BufferedData, Meta] extends Module {

  // right now, transform seems only need to be measured by elapsed time, no other metrics in mind yet
  def transform(input: I): (O,Meta)


  def inBytes(meta: Meta): Long
  def inRecords(meta: Meta): Long
  def outBytes(meta: Meta): Long
  def outRecords(meta: Meta): Long

  final def execute(input: I): (O,Meta) = {
    Try(transform(input))
    match {
      case Success((out,meta)) => {


        metricUpdate(
          Map(
            MetricArgs.InRecords->inRecords(meta),
            MetricArgs.InBytes->inBytes(meta),
            MetricArgs.InRecords->outRecords(meta),
            MetricArgs.InBytes->outBytes(meta)
          )
        )

        (out,meta)
        // TODO instrumentation

      }
      case Failure(f) => throw f
    }
  }
  override def moduleType: ModuleType.Value = ModuleType.Transformer
}
