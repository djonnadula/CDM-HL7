package com.hca.cdm.utils

import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime, LocalTime, YearMonth}
import java.util.Date
import com.hca.cdm.sys_timeZone
import com.hca.cdm.log.Logg
import com.hca.cdm.utils.DateConstants._
import scala.collection.mutable

/**
  * Created by Devaraj Jonnadula on 8/15/2016.
  *
  * Utils for Date Operations
  */
object DateUtil extends Logg {


  private lazy val formatter = {
    val temp = new SimpleDateFormat(HL7_DEFAULT)
    temp setTimeZone sys_timeZone
    temp
  }
  private lazy val FORMATTERS = new mutable.HashMap[String, DateTimeFormatter]

  def currentTimeStamp: String = formatter.format(new Date())

  def yearMonthFromString(date: String, pattern: String): YearMonth = {
    try return YearMonth.parse(date, getFormatter(pattern))
    catch {
      case e: Throwable => error("Error during YearMonth format conversion", e)
    }
    null
  }

  def dateToString(d: LocalDate, format: String): String = {
    try return d.format(getFormatter(format))
    catch {
      case e: Throwable => error("Error during date format conversion", e)
    }
    null
  }
  def dateToString(d: LocalDateTime, format: String): String = {
    try return d.format(getFormatter(format))
    catch {
      case e: Throwable => error("Error during date format conversion", e)
    }
    null
  }


  def dateFromString(date: String, pattern: String): LocalDate = LocalDate.parse(date, getFormatter(pattern))


  def getStringValueFromTime(time: LocalTime, format: String): String = time.format(getFormatter(format))

  private def getFormatter(pattern: String): DateTimeFormatter = {
    var formatter: DateTimeFormatter = FORMATTERS.getOrElse(pattern, null)
    if (formatter == null) {
      formatter = DateTimeFormatter.ofPattern(pattern)
      FORMATTERS += pattern -> formatter
    }
    formatter withZone sys_timeZone.toZoneId
    formatter
  }


}
