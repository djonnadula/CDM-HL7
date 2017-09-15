package com.hca.cdm.tcp

import java.net.InetSocketAddress

import akka.actor.{Actor, Props}
import akka.io.{IO, Tcp}
import akka.util.ByteString
import com.hca.cdm._
import com.hca.cdm.log.Logg

/**
  * Created by dof7475 on 8/23/2017.
  */
object AkkaTcpClient {

  def props(remote: InetSocketAddress): Props = {
    Props(classOf[AkkaTcpClient], remote)
  }
}

class AkkaTcpClient(remote: InetSocketAddress) extends Actor with Logg {
  import akka.io.Tcp._
  import context.system

  var reconnectCount = 0L
  var ackCount = 0L
  var sendCount = 0L

  case object Ack extends Event
  val manager = IO(Tcp)
  manager ! Connect(remote)

  override def preRestart(reason: Throwable, message: Option[Any]) = {
    info("Restarting")
    super.preRestart(reason, message)
  }

  override def postRestart(reason: Throwable) = {
    info("Restart completed!")
    super.postRestart(reason)
  }

  override def preStart() = info("TcpClient is alive")
  override def postStop() = info("TcpClient has shutdown")

  override def receive: Receive = {
    case CommandFailed(con: Connect) =>
      error("Connection failed")
      error(con.failureMessage.toString)
      if (reconnectCount > 10L) {
        error(s"Stopping context after trying to reconnect after $reconnectCount times")
        throw new RestartMeException
        context stop self
      } else {
        reconnectCount = inc(reconnectCount, 1)
        warn(s"Trying to reconnect: $reconnectCount")
        manager ! Connect(remote)
        Thread.sleep(2000)
      }

    case c @ Connected(remote, local) =>
      info(s"Connection to $remote succeeded")
      reconnectCount = 0
      val connection = sender
      val handler = context.actorOf(SimpleEchoHandler.props(connection, remote))
      connection ! Register(handler, keepOpenOnPeerClosed = true)

      context become ({
        case data: ByteString =>
          sendCount = inc(sendCount, 1)
          info("Sending request data: " + data.utf8String)
          info("Send count: " + sendCount)
          handler ! data
        case CommandFailed(w: Write) =>
          error("Failed to write request.")
          error(w.failureMessage.toString)
        case Received(data) =>
          info("Received response.")
          info("data: " + data.utf8String)
        case "close" =>
          info("Closing connection")
          connection ! Close
        case Ack =>
          ackCount = inc(ackCount, 1)
          info("Received ACK, count:" + ackCount)
        case _: ConnectionClosed =>
          info("Connection closed by server.")
          throw new RestartMeException
          context stop self
      }, discardOld = false)

    case _ =>
      error("Something else is happening")
  }
}
