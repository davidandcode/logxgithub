package com.creditkarma.logx.instrumentation

/**
  * Created by yongjia.wang on 11/16/16.
  */
class Status (val statusCode: StatusCode.Value, message: => String) {
}

object StatusCode extends Enumeration {
  val START, SUCCESS, FAILURE = Value
}