package com.hca.cdm.tcp

import java.net.InetSocketAddress

import akka.actor.{Actor, Props}
import akka.io.Tcp.{Connect, Event, Received}
import akka.io.{IO, Tcp}
import com.hca.cdm.log.Logg

/**
  * Created by dof7475 on 9/14/2017.
  */
object TcpConnectionHandler {
  def props(remote: InetSocketAddress): Props = {
    Props(classOf[TcpConnectionHandler], remote)
  }
}
class TcpConnectionHandler(remote: InetSocketAddress) extends Actor with Logg {
  import context.system
  val manager = IO(Tcp)
  case object Ack extends Event

  override def receive: Receive = {
    case connect: Connect =>
      info(s"Connecting to $remote")
      manager ! connect

      context.become{
        case Received(data) =>
          info("Received response.")
          info("data: " + data.utf8String)
        case Ack =>
          info("Received ACK")
      }
  }
}

