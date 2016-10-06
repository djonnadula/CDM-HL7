package com.hca.cdm.hl7.exception

/**
  * Created by Devaraj Jonnadula on 10/5/2016.
  *
  * Exception thrown at Runtime if Template Doesn't have Required INFO
  */
class NoInfoInTemplateException(message: String, t: Throwable) extends RuntimeException {

  def this(message: String) = this(message, null)

  def this(t: Throwable) = this("", t)


}
