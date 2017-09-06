package com.hca.cdm.tcp

import java.net.InetSocketAddress

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.io.{IO, Tcp}
import akka.util.ByteString
import com.hca.cdm.log.Logg
import com.hca.cdm.tcp.AkkaTcpClient.{Ping, SendMessage, sys}

//object AkkaTcpClient {
//  def props(remote: InetSocketAddress, message: String) = {
//    Props(classOf[AkkaTcpClient], remote, message)
//  }
//}

object AkkaTcpClient {
  var sys : ActorRef = _

  def actorSys(host :String,port :Int): ActorRef = synchronized{
      if(sys == null) sys = ActorSystem.create("PSGActorSystem").actorOf(Props(classOf[AkkaTcpClient], new InetSocketAddress(host,port)))
        sys
  }

  final case class SendMessage(message: ByteString)
  final case class Ping(message: String)
}

/**
  * Created by dof7475 on 8/23/2017.
  */
class AkkaTcpClient(remote: InetSocketAddress, sleepTime: Long, message: String) extends Actor with Logg {
  import akka.io.Tcp._
  import context.system

  info("Connecting to " +  remote.toString)

  val manager = IO(Tcp)
//  val opts = List[SocketOption]
//  opts += (SO.KeepAlive(true))
  manager ! Connect(remote)

  override def receive: Receive = {
    case CommandFailed(con: Connect) =>
      error("Connection failed")
      error(con.failureMessage.toString)
      context stop self

    case c @ Connected(remote, local) =>
      info(s"Connection to $remote succeeded")
      val connection = sender
      connection ! Tcp.SO.KeepAlive(true)
      connection ! Register(self, keepOpenOnPeerClosed = true)

//      Thread.sleep(sleepTime)
      info("Sending message")
      info(message)
      connection ! Write(ByteString(message))
      info("Sleeping for "+ sleepTime)
      Thread.sleep(sleepTime)

      context become {
//        case SendMessage(mes) =>
//          info("Sending message: " + mes.utf8String)
//          connection ! Write(mes)
//        case Ping(mes) =>
//          info("hello: " + mes)
//        case data: ByteString =>
//          info("Sending request data: " + data.utf8String)
//          connection ! Write(data)
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
