package com.hca.cdm.utils

import java.lang.Thread.{currentThread => ct, sleep => sleepFor}

import com.hca.cdm.log.Logg

/**
  * Created by Devaraj Jonnadula on 8/18/2016.
  *
  * Impl for Constant Retry Policy
  */
class RetryHandler(val defaultRetries: Int = 30, private val waitBetweenTries: Long = 1000) extends Logg {
  private val valid = validate
  require(valid._1, valid._2)
  @volatile private var TRIED_COUNT: Int = 0

  private def validate: (Boolean, String) = {
    val valid: Boolean = defaultRetries > 0 && waitBetweenTries > 0
    if (valid) {
      (valid, "Success")
    }
    else {
      (valid, "Invalid args either defaultRetries <0 OR waitBetweenTries <0")
    }
  }

  def tryAgain(): Boolean = {
    try sleepFor(waitBetweenTries)
    catch {
      case t: Throwable => error("Failed to Pause Thread while Re-trying :: " + ct.getName, t)
    }
    val retry = TRIED_COUNT < defaultRetries
    if (retry) {
      TRIED_COUNT += 1
    }
    retry
  }


}

object RetryHandler {

  def apply(defaultRetries: Int, waitBetweenTries: Long): RetryHandler = new RetryHandler(defaultRetries, waitBetweenTries)


}
