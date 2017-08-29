package com.hca.cdm.tcp

import java.net.InetSocketAddress

import akka.actor.{Actor, Props}
import akka.io.Tcp._
import akka.io.{IO, Tcp}
import akka.util.ByteString
import com.hca.cdm.log.Logg

object AkkaTcpClient {
  def props(remote: InetSocketAddress) = {
    Props(classOf[AkkaTcpClient], remote)
  }
}

/**
  * Created by dof7475 on 8/23/2017.
  */
class AkkaTcpClient(remote: InetSocketAddress) extends Actor with Logg {

  import context.system

  info("remote: " + remote.toString)
  info("Connecting to remote server")

  val manager = IO(Tcp)
//  val options = List[Inet.SocketOption]
//  options = TcpSO.keepAlive(true)
  manager ! Connect(remote)

//  case object MyAck extends Event

  override def receive: Receive = {
    case CommandFailed(con: Connect) =>
      error("Connection failed")
      error(con.failureMessage.toString)
      context stop self

    case c @ Connected(remote, local) =>
      info(s"Connection to $remote succeeded")
      val connection = sender
      connection ! Register(self)

//      info("Sending request data")
//      connection ! Write(ByteString(requestData))

      context become {
        case data: ByteString =>
          info("Sending request data: " + data.utf8String)
          connection ! Write(data)
        case CommandFailed(w: Write) =>
          error("Failed to write request.")
          error(w.failureMessage.toString)
//        case MyAck =>
//          //Our last write has completed
//          println("Writing request body.")
//          //Safe to write the request body now.
//          connection ! Write(ByteString(requestBody))
        case Received(data) =>
          info("Received response.")
          info("data: " + data.utf8String)
          // Fulfill the promise
//          thePromise.success(
//            data.decodeString("UTF-8"))s
        case "close" =>
          info("Closing connection")
          connection ! Close
        case _: ConnectionClosed =>
          info("Connection closed by server.")
          context stop self
      }

    case _ => error("Something else is happening")

  }
}
