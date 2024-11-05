package com.cdm.hl7.model

/**
  * Created by Devaraj Jonnadula on 8/18/2016.
  */
class DataModelException(message: String, t: Throwable) extends RuntimeException(message, t) {

  def this(message: String) = this(message, null)

  def this(t: Throwable) = this("", t)

  override def fillInStackTrace(): Throwable = super.fillInStackTrace()

  override def setStackTrace(stackTrace: Array[StackTraceElement]): Unit = super.setStackTrace(stackTrace)

  override def printStackTrace(): Unit = super.printStackTrace()

  override def initCause(cause: Throwable): Throwable = super.initCause(cause)
}
