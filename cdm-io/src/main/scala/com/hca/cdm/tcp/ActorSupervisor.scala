package com.hca.cdm.tcp

import java.net.InetSocketAddress

import akka.actor.SupervisorStrategy.{Restart, Resume, Stop}
import akka.actor.{Actor, OneForOneStrategy, Props}
import com.hca.cdm.log.Logg


/**
  * Factory method to create ActorSupervisor as recommended by the Akka documentation
  * <a href="http://doc.akka.io/docs/akka/current/scala/actors.html#recommended-practices">Akka Recommended Practices</a>
  */
object ActorSupervisor {
  /**
    * Create props for an actor of this type
    * @param remote remote client [[InetSocketAddress]]
    * @return [[Props]] for creating this actor
    */
  def props(remote: InetSocketAddress): Props = {
    Props(classOf[ActorSupervisor], remote)
  }
}

/**
  * An Akka Supervisor implementation that will forward all messages to the [[AkkaTcpClient]]
  * It will also restart the tcpActor if the connection is lost
  * @param remote remote client [[InetSocketAddress]]
  */
class ActorSupervisor(remote: InetSocketAddress) extends Actor with Logg {

  override def preStart(): Unit = info("The Supervisor is ready to supervise")
  override def postStop(): Unit = info("Bye Bye from the Supervisor")

  /**
    * Default behavior when an a child actor encounters an exception
    * @return specified [[akka.actor.SupervisorStrategy.Directive]] for exception thrown
    */
  override def supervisorStrategy: OneForOneStrategy = OneForOneStrategy() {
    case _: RestartMeException => Restart
    case _: ResumeMeException => Resume
    case _: StopMeException => Stop
  }

  // create the tcpActor to receive all incoming messages
  val tcpActor = context.actorOf(AkkaTcpClient.props(remote), "tcpActor")

  // actor behavior
  override def receive: Receive = {
    case msg => tcpActor forward msg
  }
}