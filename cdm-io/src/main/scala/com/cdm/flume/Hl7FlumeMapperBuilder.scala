package com.cdm.flume

import org.apache.flume.Context
import org.apache.flume.interceptor.Interceptor
import org.apache.flume.interceptor.Interceptor.Builder

/**
  * Created by Devaraj Jonnadula on 11/3/2016.
  */
private[flume] class Hl7FlumeMapperBuilder extends Builder {

  override def build(): Interceptor = {
    new Hl7FlumeMapper()
  }

  override def configure(context: Context): Unit = {
    context.getParameters
  }

}
