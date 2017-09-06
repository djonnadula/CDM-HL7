import java.net.InetSocketAddress

import akka.actor.{Actor, ActorSystem, Props}
import akka.io.{IO, Tcp}
import akka.util.ByteString
import com.hca.cdm.log.Logg
import AkkaTcpClientTester.{Ping, SendMessage}

object AkkaTcpClientTester {
//  var prop : Props = _
//  def props(host: String, port :Int) = {
//    println("test")
//    if(prop == null) prop = Props(classOf[AkkaTcpClientTester], new InetSocketAddress(host, port))
//    prop
//  }

  def props(remote: InetSocketAddress): Props = {
    println("test")
    Props(new AkkaTcpClientTester(remote))
  }

  final case class SendMessage(message: ByteString)
  final case class Ping(message: String)
}


class AkkaTcpClientTester(remote: InetSocketAddress) extends Actor with Logg {
  import akka.io.Tcp._
  import context.system

  println("Connecting to " +  remote.toString)

  val manager = IO(Tcp)
  println("manager.path: " + manager.path.toString)
  //  val options = List[Inet.SocketOption]
  //  options -> Tcp.SO.KeepAlive(true)
  manager ! Connect(remote)

  println("here")
  //  case object MyAck extends Event
//  context.children.foreach(ref => println("context.actor: " + ref.path.toString))

  override def receive: Receive = {
    case CommandFailed(con: Connect) =>
      //      listener ! "connection failed"
      println("Connection failed")
      println(con.failureMessage.toString)
      context stop self

    case c @ Connected(remote, local) =>
      println(s"Connection to $remote succeeded")
      println(s"local $local")
      //      listener ! c
      println(c.localAddress.toString)
      println(c.remoteAddress.toString)
      val connection = sender
      connection ! Register(self)
      println("connection.path: " + connection.path.toString)
      println("self.path: " + self.path.toString)

      //      println("Sending request data")
      //      connection ! Write(ByteString(message))

      context become {
        case SendMessage(message) =>
          println("Sending message: " + message.utf8String)
          connection ! Write(message)
        case Ping(message) =>
          println("hello: " + message)
        case data: ByteString =>
          println("Sending request data: " + data.utf8String)
          connection ! Write(data)
        case CommandFailed(w: Write) =>
          println("Failed to write request.")
          println(w.failureMessage.toString)
        //          listener ! "command failed"
        //        case MyAck =>
        //          //Our last write has completed
        //          println("Writing request body.")
        //          //Safe to write the request body now.
        //          connection ! Write(ByteString(requestBody))
        case Received(data) =>
          println("Received response.")
          println("data: " + data.utf8String)
        //          listener ! data
        // Fulfill the promise
        //          thePromise.success(
        //            data.decodeString("UTF-8"))s
        case "close" =>
          println("Closing connection")
          connection ! Close
        case _: ConnectionClosed =>
          println("Connection closed by server.")
          //          listener ! "connection closed"
          context stop self
      }

    case e: Exception =>
      println(e.printStackTrace().toString)

    case t: Throwable =>
      println(t)

    case _ =>
      println("Something else is happening")

  }

}

val socketAddress = new InetSocketAddress("localhost", 8081)
val actorSys = ActorSystem.create("PSGActorSystem")
val tcpActor = actorSys.actorOf(AkkaTcpClientTester.props(socketAddress), "tcpActor")
Thread.sleep(1000)
tcpActor ! SendMessage(ByteString("hello"))
Thread.sleep(1000)
tcpActor ! Ping("hello")
Thread.sleep(1000)
tcpActor ! "close"

tcpActor ! SendMessage(ByteString("arg"))
