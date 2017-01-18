package com.hca.cdm.hl7.exception

/**
  * Created by Devaraj Jonnadula on 10/12/2016.
  *
  * Exception thrown at Runtime if Template Doesn't have Required INFO
  */
class TemplateInfoException(message: String, t: Throwable) extends RuntimeException(message,t) {

  def this(message: String) = this(message, null)

  def this(t: Throwable) = this("", t)


}

