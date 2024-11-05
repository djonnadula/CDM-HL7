package com.cdm.tcp

import java.net.InetSocketAddress

import akka.actor.{Actor, Props}
import akka.io.{IO, Tcp}
import akka.util.ByteString
import com.cdm._
import com.cdm.log.Logg

/**
  * Factory method to create ActorSupervisor as recommended by the Akka documentation
  * <a href="http://doc.akka.io/docs/akka/current/scala/actors.html#recommended-practices">Akka Recommended Practices</a>
  */
object AkkaTcpClient {
  /**
    * Create props for an actor of this type
    * @param remote remote client [[InetSocketAddress]]
    * @return [[Props]] for creating this actor
    */
  def props(remote: InetSocketAddress): Props = {
    Props(classOf[AkkaTcpClient], remote)
  }
}

/**
  * Tcp Client Implementation
  * @param remote
  */
class AkkaTcpClient(remote: InetSocketAddress) extends Actor with Logg {
  import akka.io.Tcp._
  import context.system

  var reconnectCount = 0L
  var ackCount = 0L
  var sendCount = 0L

  case object Ack extends Event
  val manager = IO(Tcp)
  manager ! Connect(remote)

  /**
    * Before restart behavior
    * @param reason reason to restart the actor
    * @param message possible message
    */
  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    info("Restarting")
    super.preRestart(reason, message)
  }

  /**
    * After restart behavior
    * @param reason possible reason
    */
  override def postRestart(reason: Throwable): Unit = {
    info("Restart completed!")
    super.postRestart(reason)
  }

  /**
    * Before start behavior
    */
  override def preStart(): Unit = info("TcpClient is alive")

  /**
    * After start behavior
    */
  override def postStop(): Unit = info("TcpClient has shutdown")

  // actor behavior
  override def receive: Receive = {
    // tries to reconnect 10 times before restarting
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
      val handler = context.actorOf(SimplisticHandler.props(connection, remote))
      connection ! Register(handler, keepOpenOnPeerClosed = true)

      context become ({
        case data: ByteString =>
          sendCount = inc(sendCount, 1)
          info("Passing data to handler")
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
          info("Received ACK")
        case _: ConnectionClosed =>
          info("Connection closed by server.")
          throw new RestartMeException
          context stop self
      }, discardOld = false)

    case _ =>
      error("Something else is happening")
  }
}
