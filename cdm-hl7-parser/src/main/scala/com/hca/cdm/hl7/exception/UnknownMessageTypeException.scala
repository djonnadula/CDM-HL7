package com.hca.cdm.hl7.exception

/**
  * Created by Devaraj Jonnadula on 9/26/2016.
  *
  *  Exception thrown at Runtime if Registered Handlers Cannot deal with HL7 Came In
  */
class UnknownMessageTypeException (message: String, t: Throwable) extends RuntimeException(message, t) {

  def this(message: String) = this(message, null)

  def this(t: Throwable) = this("", t)



}