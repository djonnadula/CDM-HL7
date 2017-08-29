package com.hca.cdm.job.psg.aco

import java.net.InetSocketAddress

import akka.actor.{ActorSystem, Props}
import com.hca.cdm.log.Logg
import com.hca.cdm.tcp.AkkaTcpClient


/**
  * Created by dof7475 on 8/24/2017.
  */
object AkkaTest extends App with Logg {

  override def main(args: Array[String]): Unit = {
    val sys = ActorSystem.create("PSGActorSystem")
    val akkaProps = Props(classOf[AkkaTcpClient], new InetSocketAddress("xrdclpintsin01t.unix.medcity.net", 15713), "test")
    val tcpActor = sys.actorOf(akkaProps)
    info("tcpActor.path: " + tcpActor.path)
  }

}

