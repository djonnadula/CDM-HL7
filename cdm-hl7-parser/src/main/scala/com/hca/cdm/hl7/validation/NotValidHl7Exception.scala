package com.hca.cdm.hl7.validation

/**
  * Created by Devaraj Jonnadula on 9/20/2016.
  *
  *  exception thrown at Runtime if HL7 doesn't have valid Required INFO
  */
class NotValidHl7Exception(message: String, t: Throwable) extends RuntimeException(message, t) {

  def this(message: String) = this(message, null)

  def this(t: Throwable) = this("", t)

}
