package com.hca.cdm.tcp

/**
  * Protocol to restart actors
  */
object RestartProtocol {

  /**
    * Message class
    * @param msg message
    */
  case class Message(msg: String)
}

/**
  * RestartMeException implementation
  */
class RestartMeException extends Exception("RESTART")

/**
  * ResumeMeException implementation
  */
class ResumeMeException extends Exception("RESUME")

/**
  * StopMeException implementation
  */
class StopMeException extends Exception("STOP")