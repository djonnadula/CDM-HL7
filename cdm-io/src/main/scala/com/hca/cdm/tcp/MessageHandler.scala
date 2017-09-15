package com.hca.cdm.tcp

import java.net.InetSocketAddress

import akka.actor.{Actor, DeadLetter, Props}
import akka.io.Tcp.Write
import akka.util.ByteString
import com.hca.cdm.log.Logg

object MessageHandler {
  def props(remote: InetSocketAddress): Props = {
    Props(classOf[MessageHandler], remote)
  }
}

class MessageHandler(remote: InetSocketAddress) extends Actor with Logg {
  val tcpClient = context.actorOf(AkkaTcpClient.props(remote), "dlTcpActor")

  def receive = {
    case w: Write =>
      info("Write: " + w.data.utf8String)
      tcpClient ! w.data
    case b: ByteString =>
      info("ByteString: " + b.utf8String)
      tcpClient ! b
    case d: DeadLetter =>
      info("Message: " + d.message.toString)
      info("Recipient: " + d.recipient.path.toString)
      info("Sender: " + d.sender.path.toString)
    case _ => info("something else")
  }
}



