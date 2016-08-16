package com.hca.cdm.utils

import java.text.SimpleDateFormat
import java.time.{LocalDate, LocalTime, YearMonth}
import java.time.format.DateTimeFormatter
import java.util.Date
import scala.collection.mutable.HashMap
import com.hca.cdm.utils.DateConstants._
import com.hca.cdm.log.Logger

/**
  * Created by Devaraj Jonnadula on 8/15/2016.
  */
object DateUtil extends Logger {


  private lazy val formatter = new SimpleDateFormat(HL7_DEFAULT)
  private lazy val FORMATTERS = new HashMap[String, DateTimeFormatter]

  def currentTimeStamp = formatter.format(new Date())

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


  def dateFromString(date: String, pattern: String): LocalDate = LocalDate.parse(date, getFormatter(pattern))


  def getStringValueFromTime(time: LocalTime, format: String): String = time.format(getFormatter(format))

  private def getFormatter(pattern: String): DateTimeFormatter = {
    var formatter: DateTimeFormatter = FORMATTERS.getOrElse(pattern, null)
    if (formatter == null) {
      formatter = DateTimeFormatter.ofPattern(pattern)
      FORMATTERS += pattern -> formatter
    }
    formatter
  }


}
