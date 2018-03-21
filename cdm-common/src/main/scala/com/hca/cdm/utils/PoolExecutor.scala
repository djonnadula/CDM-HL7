package com.hca.cdm.utils

import java.util.concurrent.Executor

/**
  * Created by Devaraj Jonnadula on 9/23/2016.
  */
trait PoolExecutor extends Executor{

  def shutDown(): Unit

  def printStats(): Unit

  def tasksPending: Boolean

  def canExecuteMore: Boolean

  def tryWaitUntilAllTasksCompleted: Boolean

}
