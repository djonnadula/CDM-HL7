package com.hca.cdm.hl7.exception

/**
  * Created by Devaraj Jonnadula on 10/13/2016.
  *
  * Exception thrown at Runtime if HL7 format is not Valid.
  */
class InvalidHl7FormatException(message: String, t: Throwable) extends RuntimeException {

  def this(message: String) = this(message, null)

  def this(t: Throwable) = this("", t)


}