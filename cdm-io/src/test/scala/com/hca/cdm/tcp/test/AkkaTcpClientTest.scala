package com.hca.cdm.tcp.test

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

/**
  * Created by dof7475 on 8/25/2017.
  */
class AkkaTcpClientTest extends TestKit(ActorSystem("Test")) with ImplicitSender with WordSpecLike with Matchers with BeforeAndAfterAll {

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  "An Echo actor" must {

    "send back messages unchanged" in {
//      val actorRef = TestActorRef[AkkaTcpClient]
//      actorRef ! "hello world"
////      val actor = actorRef.underlyingActor
////      val echo = system.actorOf(actorRef.props)
////      echo ! "hello world"
//      expectMsg("hello world")
    }

  }
}
