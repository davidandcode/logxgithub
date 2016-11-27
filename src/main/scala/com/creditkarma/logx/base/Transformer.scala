package com.creditkarma.logx.base

import scala.util.{Failure, Success, Try}

/**
  * Created by yongjia.wang on 11/16/16.
  */
trait Transformer[I <: BufferedData, O <: BufferedData] extends Module {

  // right now, transform seems only need to be measured by elapsed time, no other metrics in mind yet
  def transform(input: I): O

  def inRecords(input:I):Long
  def outRecords(output:O):Long

  final def execute(input: I): O = {
    Try(transform(input))
    match {
      case Success(out) => {

        metricUpdate(
          Map(
            MetricArgs.InRecords->inRecords(input),
            MetricArgs.OutRecords->outRecords(out)
          )
        )



        out
        // TODO instrumentation
      }
      case Failure(f) => throw f
    }
  }
  override def moduleType: ModuleType.Value = ModuleType.Transformer
}
