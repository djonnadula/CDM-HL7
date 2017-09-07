package com.hca.cdm.tcp

import java.net.InetSocketAddress

import akka.actor.{Actor, Props}
import akka.io.{IO, Tcp}
import akka.util.ByteString
import com.hca.cdm.log.Logg
import com.hca.cdm.tcp.AkkaTcpClient.SendMessage


/**
  * Created by dof7475 on 8/23/2017.
  */
object AkkaTcpClient {

  def props(host: String, port :Int, sleepTime: Long): Props = {
    Props(classOf[AkkaTcpClient], new InetSocketAddress(host, port), sleepTime)
  }

  final case class SendMessage(message: ByteString)
  final case class Ping(message: String)
}

class AkkaTcpClient(remote: InetSocketAddress, sleepTime: Long) extends Actor with Logg {
  import akka.io.Tcp._
  import context.system

  info("Connecting to " +  remote.toString)

  val manager = IO(Tcp)
  manager ! Connect(remote)

  override def receive: Receive = {
    case CommandFailed(con: Connect) =>
      error("Connection failed")
      error(con.failureMessage.toString)
      context stop self

    case c @ Connected(remote, local) =>
      info(s"Connection to $remote succeeded")
      val handler = context.actorOf(SimplisticHandler.props())
      val connection = sender
      connection ! Register(handler, keepOpenOnPeerClosed = true)
      info("handler.path: " + handler.path)

      context become {
        case SendMessage(mes) =>
          info("Sending message: " + mes.utf8String)
          connection ! Write(mes)
        case data: ByteString =>
          info("Sending request data: " + data.utf8String)
          connection ! Write(data)
        case CommandFailed(w: Write) =>
          error("Failed to write request.")
          error(w.failureMessage.toString)
        case Received(data) =>
          info("Received response.")
          info("data: " + data.utf8String)
        case "close" =>
          info("Closing connection")
          connection ! Close
        case e: Exception =>
          error(e.printStackTrace().toString)
        case t: Throwable =>
          error(t)
        case _: ConnectionClosed =>
          info("Connection closed by server.")
          context stop self
      }

    case _ =>
      error("Something else is happening")

  }
}
