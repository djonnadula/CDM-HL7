package com.hca.cdm.log

import org.apache.log4j.Logger

/**
  * Created by Devaraj Jonnadula on 8/11/2016.
  */
trait Logger {

  protected val loggerName = this.getClass.getName
  protected lazy val logger = Logger.getLogger(loggerName)
  protected var logIdent: String = _

  private def msgWithLogIdent(msg: String) = if (logIdent == null) msg else logIdent + msg

  def trace(msg: => String): Unit = if (logger.isTraceEnabled) logger.trace(msgWithLogIdent(msg))

  def trace(e: => Throwable): Any = if (logger.isTraceEnabled) logger.trace(logIdent, e)

  def trace(msg: => String, e: => Throwable): Unit = if (logger.isTraceEnabled) logger.trace(msgWithLogIdent(msg), e)

  def debug(msg: => String): Unit = if (logger.isDebugEnabled) logger.debug(msgWithLogIdent(msg))

  def debug(e: => Throwable): Any = if (logger.isDebugEnabled) logger.debug(logIdent, e)

  def debug(msg: => String, e: => Throwable): Unit = if (logger.isDebugEnabled) logger.debug(msgWithLogIdent(msg), e)

  def info(msg: => String): Unit = if (logger isInfoEnabled()) logger.info(msgWithLogIdent(msg))

  def info(e: => Throwable): Any = if (logger.isInfoEnabled) logger.info(logIdent, e)

  def info(msg: => String, e: => Throwable): Unit = if (logger.isInfoEnabled) logger.info(msgWithLogIdent(msg), e)

  def warn(msg: => String): Unit = logger.warn(msgWithLogIdent(msg))

  def warn(e: => Throwable): Any = logger.warn(logIdent, e)

  def warn(msg: => String, e: => Throwable): Unit = logger.warn(msgWithLogIdent(msg), e)

  def error(msg: => String): Unit = logger.error(msgWithLogIdent(msg))

  def error(e: => Throwable): Any = logger.error(logIdent, e)

  def error(msg: => String, e: => Throwable): Unit = logger.error(msgWithLogIdent(msg), e)

  def fatal(msg: => String): Unit = logger.fatal(msgWithLogIdent(msg))

  def fatal(e: => Throwable): Any = logger.fatal(logIdent, e)

  def fatal(msg: => String, e: => Throwable): Unit = logger.fatal(msgWithLogIdent(msg), e)

}
