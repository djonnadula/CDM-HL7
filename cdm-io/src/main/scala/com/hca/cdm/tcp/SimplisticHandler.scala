package com.hca.cdm.tcp

import java.net.InetSocketAddress

import akka.actor.{Actor, ActorRef, Props}
import akka.io.Tcp
import akka.util.ByteString
import com.hca.cdm.log.Logg

/**
  * Created by dof7475 on 9/11/2017.
  */

object SimplisticHandler {
  def props(connection: ActorRef, remote: InetSocketAddress): Props = {
    Props(classOf[SimplisticHandler], connection, remote)
  }
}

class SimplisticHandler(connection: ActorRef, remote: InetSocketAddress)
  extends Actor with Logg {

  import Tcp._

  // sign death pact: this actor terminates when connection breaks
  context watch connection

  case object Ack extends Event

  def receive = {
    case Received(data) =>
      info("Received data: \n\t" + data.utf8String)
    case data: ByteString =>
      info("Received ByteString")
//      buffer(data)
      info("Writing ByteString: \n\t" + data.utf8String)
      connection ! Write(data, Ack)
    case Ack =>
      info("Received ACK")
//      acknowledge()
    case PeerClosed =>
      info("Peer closed")
      connection ! "close"
      context stop self
    case _ =>
      info("Echo Handler doesn't know what to do")
  }

  override def postStop(): Unit = {
    info(s"transferred $transferred bytes from/to [$remote]")
  }

  var storage = Vector.empty[ByteString]
  var stored = 0L
  var transferred = 0L
  var closing = false

  val maxStored = 100000000L
  val highWatermark = maxStored * 5 / 10
  val lowWatermark = maxStored * 3 / 10
  var suspended = false

  private def buffer(data: ByteString): Unit = {
    storage :+= data
    stored += data.size
    info("doing some buffering")
    info(s"stored: $stored")

    if (stored > maxStored) {
      warn(s"drop connection to [$remote] (buffer overrun)")
      context stop self

    } else if (stored > highWatermark) {
      debug(s"suspending reading")
      connection ! SuspendReading
      suspended = true
    }
  }

  private def acknowledge(): Unit = {
    require(storage.nonEmpty, "storage was empty")

    val size = storage(0).size
    stored -= size
    transferred += size

    storage = storage drop 1

    if (suspended && stored < lowWatermark) {
      debug("resuming reading")
      connection ! ResumeReading
      suspended = false
    }

    if (storage.isEmpty) {
      if (closing) context stop self
      else context.unbecome()
    } else {
      info("Writing something in acknowledge: " + storage(0).utf8String)
      connection ! Write(storage(0), Ack)
    }
  }
}