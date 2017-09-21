package com.hca.cdm.tcp

import akka.actor.{Actor, DeathPactException, Props}
import com.hca.cdm.log.Logg

/**
  * Created by dof7475 on 9/12/2017.
  */
object Supervisor {
  def props: Props = {
    Props(classOf[Supervisor])
  }
}

class Supervisor extends Actor with Logg {
  import akka.actor.OneForOneStrategy
  import akka.actor.SupervisorStrategy._
  import scala.concurrent.duration._

  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 10, withinTimeRange = 1 minute) {
      case d: DeathPactException =>
        error(d)
        Restart
      case a: ArithmeticException =>
        info(a)
        Resume
      case n: NullPointerException =>
        error(n)
        Restart
      case i: IllegalArgumentException =>
        error(i)
        Stop
      case e: Exception =>
        error(e)
        Escalate
    }

  def receive: PartialFunction[Any, Unit] = {
    case p: Props => sender ! context.actorOf(p)
  }
}
