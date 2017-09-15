package com.hca.cdm.tcp

/**
  * Created by dof7475 on 9/14/2017.
  */
object RestartProtocol {
  case class Message(msg: String)
}

class RestartMeException extends Exception("RESTART")
class ResumeMeException extends Exception("RESUME")
class StopMeException extends Exception("STOP")