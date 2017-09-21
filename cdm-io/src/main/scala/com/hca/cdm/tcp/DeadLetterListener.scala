package com.hca.cdm.tcp
import java.net.InetSocketAddress

import akka.actor.{Actor, DeadLetter, Props}
import akka.util.ByteString
import com.hca.cdm.log.Logg

object DeadLetterListener {
  def props(host: String, port :Int): Props = {
    Props(classOf[DeadLetterListener], new InetSocketAddress(host, port))
  }
}

class DeadLetterListener(remote: InetSocketAddress) extends Actor with Logg {
  val handler = context.actorOf(MessageHandler.props(remote), "messageHandler")

  def receive: PartialFunction[Any, Unit] = {
    case d: DeadLetter =>
      handler ! d.message
    case b: ByteString =>
      handler ! b
    case _ => info("something else")
  }
}


