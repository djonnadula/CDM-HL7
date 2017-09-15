package com.hca.cdm.tcp

import java.net.InetSocketAddress

import akka.actor.SupervisorStrategy.{Restart, Resume, Stop}
import akka.actor.{Actor, OneForOneStrategy, Props}
import com.hca.cdm.log.Logg


/**
  * Created by dof7475 on 9/14/2017.
  */
object ActorSupervisor {
  def props(remote: InetSocketAddress): Props = {
    Props(classOf[ActorSupervisor], remote)
  }
}

class ActorSupervisor(remote: InetSocketAddress) extends Actor with Logg {
  override def preStart() = info("The Supervisor is ready to supervise")
  override def postStop() = info("Bye Bye from the Supervisor")

  override def supervisorStrategy = OneForOneStrategy() {
    case _: RestartMeException => Restart
    case _: ResumeMeException => Resume
    case _: StopMeException => Stop
  }

  val tcpActor = context.actorOf(AkkaTcpClient.props(remote), "tcpActor")

  override def receive: Receive = {
    case msg => tcpActor forward msg
  }
}