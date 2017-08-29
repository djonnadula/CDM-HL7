package com.hca.cdm.tcp

import akka.actor.{Actor, Props}
import com.hca.cdm.log.Logg

/**
  * Created by dof7475 on 8/25/2017.
  */
object AkkaTcpListener {
  def props() = {
    Props(classOf[AkkaTcpListener])
  }
}

class AkkaTcpListener() extends Actor with Logg {
  override def receive: Receive = {
    case "connection failed" => info("connection failed")
    case _ => info("listener doing stuff")
  }
}
