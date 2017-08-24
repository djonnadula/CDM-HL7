package com.hca.cdm

import java.util.concurrent.TimeUnit.{HOURS, MILLISECONDS}
import scala.collection.concurrent.TrieMap

/**
  * Created by Devaraj Jonnadula on 7/17/2017.
  */
trait ReferenceDataHandler[K, Model] {

  protected var refreshCache: Boolean = true
  private val Cache_Store = new TrieMap[K, Model]
  private val resetIntervalHours = 24
  private val cacheRefresher = newDaemonScheduler("cacheRefresher")
  cacheRefresher.scheduleAtFixedRate(runnable(reset()), MILLISECONDS.convert(resetIntervalHours, HOURS), MILLISECONDS.convert(resetIntervalHours, HOURS), MILLISECONDS)
  registerHook(newThread("cacheRefresher-SHook", runnable(stop())))


  private def reset(): Unit = synchronized {
    if (refreshCache) Cache_Store clear()
  }

  private def stop(): Unit = {
    cacheRefresher.shutdownNow()
  }


}
