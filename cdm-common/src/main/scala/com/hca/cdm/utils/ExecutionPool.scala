package com.hca.cdm.utils

import java.util.concurrent.{RejectedExecutionHandler, ThreadPoolExecutor, TimeUnit}
import com.hca.cdm._
import com.hca.cdm.log.Logg
import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}

/**
  * Created by Devaraj Jonnadula on 9/23/2016.
  *
  * Impl for Pool to tie up with Segements Handle for Tasks to execute in it's own pool
  */
class ExecutionPool extends Logg with PoolExecutor {

  logIdent = " Thread Pool Handler "

  private lazy val pool = {
    val t = newDaemonCachedThreadPool(logIdent)
    t.setMaximumPoolSize(60)
    t.setRejectedExecutionHandler(new PoolFullHandler)
    t
  }

  def getExecutionPool: ThreadPoolExecutor = pool

  private def logEx(t: Throwable): Unit = error("Unable to Execute Task " + currThread.getName, t)

  implicit val poolContext: ExecutionContextExecutor = ExecutionContext.fromExecutor(pool, logEx)

  override def shutDown(): Unit = {
    pool.shutdown()
    pool.awaitTermination(1, TimeUnit.HOURS)
    info(" Shutdown Completed Gracefully for " + logIdent)
  }

  override def printStats(): Unit = pool.synchronized {
    info("Total Tasks Completed So far        :: " + pool.getCompletedTaskCount)
    info("Total Tasks Currently Executing     :: " + pool.getActiveCount)
    info("Total Tasks Scheduled for Execution :: " + pool.getTaskCount)
  }

  private class PoolFullHandler extends RejectedExecutionHandler {

    private val failureTracker = new TrieMap[Runnable, Int]()

    override def rejectedExecution(r: Runnable, executor: ThreadPoolExecutor): Unit = {
      debug(" Task Cannot Execute and Trying to Run again from Pool :: " + executor)
      executor.purge()
      if (!failureTracker.isDefinedAt(r)) failureTracker += r -> 1
      else failureTracker(r) match {
        case x => if (x < 10) {
          failureTracker update(r, x + 1)
          sleep(500)
          executor submit r
        }
        else if (x > 20) failureTracker remove r
        else {
          sleep(1500)
          executor submit r
          failureTracker update(r, x + 1)
        }
      }

    }
  }

  override def tasksPending: Boolean = pool.synchronized {
    pool.getActiveCount == 0 & pool.getTaskCount == 0
  }

  override def canExecuteMore: Boolean = pool.synchronized(!pool.isShutdown)

  override def tryWaitUntilAllTasksCompleted: Boolean = {
    pool.purge()
    while (!pool.getQueue.isEmpty) {
      info("Waiting on Pool for tasks to be completed for " + logIdent + " Pending Tasks So far :: " + pool.getQueue.size())
    }
    tasksPending
  }


}
