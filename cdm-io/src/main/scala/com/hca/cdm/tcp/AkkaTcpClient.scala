package com.hca.cdm.tcp

import java.net.InetSocketAddress

import akka.actor.{Actor, Props}
import akka.io.{IO, Tcp}
import akka.util.ByteString
import com.hca.cdm.log.Logg
import com.hca.cdm.tcp.AkkaTcpClient.{Ping, SendMessage}

//object AkkaTcpClient {
//  def props(remote: InetSocketAddress, message: String) = {
//    Props(classOf[AkkaTcpClient], remote, message)
//  }
//}

object AkkaTcpClient {
  var prop : Props = _
  def props(host :String,port :Int) = {
    if(prop == null) prop = Props(classOf[AkkaTcpClient], new InetSocketAddress(host,port))
    prop
  }

  final case class SendMessage(message: ByteString)
  final case class Ping(message: String)
}

/**
  * Created by dof7475 on 8/23/2017.
  */
class AkkaTcpClient(remote: InetSocketAddress, message: String) extends Actor with Logg {
  import akka.io.Tcp._
  import context.system

  info("Connecting to " +  remote.toString)

  val manager = IO(Tcp)
  info("manager.path: " + manager.path.toString)
//  val options = List[Inet.SocketOption]
//  options -> Tcp.SO.KeepAlive(true)
  manager ! Connect(remote)
  
//  case object MyAck extends Event
  context.children.foreach(ref => info("context.actor: " + ref.path.toString))

  override def receive: Receive = {
    case CommandFailed(con: Connect) =>
//      listener ! "connection failed"
      error("Connection failed")
      error(con.failureMessage.toString)
      context stop self

    case c @ Connected(remote, local) =>
      info(s"Connection to $remote succeeded")
      info(s"local $local")
//      listener ! c
      info(c.localAddress.toString)
      info(c.remoteAddress.toString)
      val connection = sender
      connection ! Register(self)
      info("connection.path: " + connection.path.toString)
      info("self.path: " + self.path.toString)

      info("Sending request data")
      connection ! Write(ByteString(message))

      context become {
        case SendMessage(message) =>
          info("Sending message: " + message.utf8String)
          connection ! Write(message)
        case Ping(message) =>
          info("hello: " + message)
        case data: ByteString =>
          info("Sending request data: " + data.utf8String)
          connection ! Write(data)
        case CommandFailed(w: Write) =>
          error("Failed to write request.")
          error(w.failureMessage.toString)
//          listener ! "command failed"
//        case MyAck =>
//          //Our last write has completed
//          println("Writing request body.")
//          //Safe to write the request body now.
//          connection ! Write(ByteString(requestBody))
        case Received(data) =>
          info("Received response.")
          info("data: " + data.utf8String)
//          listener ! data
          // Fulfill the promise
//          thePromise.success(
//            data.decodeString("UTF-8"))s
        case "close" =>
          info("Closing connection")
          connection ! Close
        case _: ConnectionClosed =>
          info("Connection closed by server.")
//          listener ! "connection closed"
          context stop self
      }

    case e: Exception =>
      error(e.printStackTrace().toString)

    case _ =>
      error("Something else is happening")

  }
}
