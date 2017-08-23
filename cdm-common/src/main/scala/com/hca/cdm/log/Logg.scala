package com.hca.cdm.log

import org.apache.log4j.Logger

/**
  * Created by Devaraj Jonnadula on 8/11/2016.
  *
  * Impl for Logging
  */
trait Logg {

  protected val loggerName: String = this.getClass.getName
  @transient protected lazy val logger: Logger = Logger.getLogger(loggerName)
  protected var logIdent: String = _

  private def msgWithLogIdent(msg: AnyRef) = if (logIdent == null) msg else logIdent + msg

  protected  def trace(msg: => AnyRef): Unit = if (logger.isTraceEnabled) logger.trace(msgWithLogIdent(msg))

  protected def trace(e: => Throwable): Any = if (logger.isTraceEnabled) logger.trace(logIdent, e)

  protected def trace(msg: => AnyRef, e: => Throwable): Unit = if (logger.isTraceEnabled) logger.trace(msgWithLogIdent(msg), e)

  protected def debug(msg: => AnyRef): Unit = if (logger.isDebugEnabled) logger.debug(msgWithLogIdent(msg))

  protected def debug(e: => Throwable): Any = if (logger.isDebugEnabled) logger.debug(logIdent, e)

  protected def debug(msg: => AnyRef, e: => Throwable): Unit = if (logger.isDebugEnabled) logger.debug(msgWithLogIdent(msg), e)

  protected def info(msg: => AnyRef): Unit = if (logger isInfoEnabled()) logger.info(msgWithLogIdent(msg))

  protected def info(e: => Throwable): Any = if (logger.isInfoEnabled) logger.info(logIdent, e)

  protected def info(msg: => AnyRef, e: => Throwable): Unit = if (logger.isInfoEnabled) logger.info(msgWithLogIdent(msg), e)

  protected def warn(msg: => AnyRef): Unit = logger.warn(msgWithLogIdent(msg))

  protected def warn(e: => Throwable): Any = logger.warn(logIdent, e)

  protected def warn(msg: => AnyRef, e: => Throwable): Unit = logger.warn(msgWithLogIdent(msg), e)

  protected def error(msg: => AnyRef): Unit = logger.error(msgWithLogIdent(msg))

  protected def error(e: => Throwable): Any = logger.error(logIdent, e)

  protected def error(msg: => AnyRef, e: => Throwable): Unit = logger.error(msgWithLogIdent(msg), e)

  protected def fatal(msg: => AnyRef): Unit = logger.fatal(msgWithLogIdent(msg))

  protected def fatal(e: => Throwable): Any = logger.fatal(logIdent, e)

  protected def fatal(msg: => AnyRef, e: => Throwable): Unit = logger.fatal(msgWithLogIdent(msg), e)

}
