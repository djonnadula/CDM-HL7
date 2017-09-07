package com.hca.cdm.tcp

import akka.actor.{Actor, Props}
import akka.io.Tcp
import com.hca.cdm.log.Logg
import com.hca.cdm.tcp.SimplisticHandler.Hello


/**
  * Created by dof7475 on 9/1/2017.
  */
object SimplisticHandler {
  def props(): Props = {
    Props(classOf[SimplisticHandler])
  }

  final case class Hello(msg: String)
}


class SimplisticHandler extends Actor with Logg {
  import Tcp._
  override def receive: Receive = {
    case Hello(msg) => info("hello")
    case Received(data) =>
      info("received response")
      info("response data: " + data.utf8String)
    case PeerClosed     =>
      info("handler says peer closed")
      context stop self
    case _ =>
      info("handler doesn't know what to do")
  }
}
