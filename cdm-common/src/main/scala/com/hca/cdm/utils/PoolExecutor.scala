package com.hca.cdm.utils

/**
  * Created by Devaraj Jonnadula on 9/23/2016.
  */
trait PoolExecutor {

  def shutDown(): Unit

  def printStats(): Unit

  def tasksPending: Boolean

  def canExecuteMore: Boolean

  def tryWaitUntilAllTasksCompleted: Boolean

}
