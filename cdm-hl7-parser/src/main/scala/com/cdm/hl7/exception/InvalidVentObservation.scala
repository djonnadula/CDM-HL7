package com.cdm.hl7.exception

/**
  * Created by Devaraj Jonnadula on 1/5/2018.
  *
  * Exception thrown at Runtime if Vent has Any Invalid Observation/Unit .
  */
class InvalidVentObservation(message: String, t: Throwable) extends RuntimeException(message,t) {

  def this(message: String) = this(message, null)

  def this(t: Throwable) = this("", t)

}
