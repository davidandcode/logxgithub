package com.creditkarma.logx.base

/**
  * Created by yongjia.wang on 11/16/16.
  */

trait Status {
  def statusCode: StatusCode.Value
  def message: String
  override def toString: String = s"$statusCode($message)"
}

class StatusOK (msg: String) extends Status {
  override val statusCode = StatusCode.OK
  override def message: String = msg
}

class StatusError(val error: Throwable) extends Status {
  val statusCode = StatusCode.ERROR
  override def message: String = s"${error.getMessage}\n${error.getStackTraceString}"
}

object StatusCode extends Enumeration {
  val OK, ERROR = Value
}
