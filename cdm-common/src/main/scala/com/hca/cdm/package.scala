package com.hca

import java.io.{File, Serializable}
import java.lang.Runtime.{getRuntime => rt}
import java.lang.Thread.UncaughtExceptionHandler
import java.net.InetAddress
import java.nio.charset.StandardCharsets
import java.time.{LocalDate, Period}
import java.util.Properties
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{Executors, ScheduledExecutorService, ThreadFactory, ThreadPoolExecutor}

import com.hca.cdm.exception.CdmException
import com.hca.cdm.log.Logg
import org.apache.commons.lang.SerializationException
import org.apache.commons.lang.SerializationUtils.{deserialize => des, serialize => ser}

import scala.collection.JavaConverters._
import scala.io.Source
import scala.language.postfixOps

/**
  * Created by Devaraj Jonnadula on 8/19/2016.
  */
package object cdm extends Logg {

  lazy val UTF8 = StandardCharsets.UTF_8
  var prop: scala.collection.mutable.Map[String, String] = _
  lazy val propFile = "CDMHL7.properties"
  lazy val EMPTYSTR = ""
  lazy val emptyArray = Array.empty[Any]
  lazy val FS = File.separator
  lazy val outStream = System.out


  def host = InetAddress.getLocalHost.getHostName

  def currMillis = System.currentTimeMillis

  def currThread = Thread.currentThread()

  def sleep(howLong: Long) = {
    try Thread.sleep(howLong)
    catch {
      case in: InterruptedException => error("Sleep Interrupted :: " + in.getMessage)
    }
  }

  def dateRange(from: LocalDate, until: LocalDate, step: Period = Period.ofDays(1)): Iterator[String] = {
    Iterator.iterate(from)(_.plus(step)).takeWhile(!_.isAfter(until)).map(date => {
      var req = date.getYear + "-0" + date.getMonthValue
      if (date.getDayOfMonth < 10) req += "-0" + date.getDayOfMonth
      else req += "-" + date.getDayOfMonth
      req
    })
  }


  def closeResource(res: AutoCloseable): Unit = if (res != null) res.close()

  def valid(d: Any, minSize: Int = 1): Boolean = {
    d match {
      case x: Array[Any] => x != null & x.length >= minSize
      case x: String => x != null
      case x: Traversable[Any] => x != null & x.nonEmpty
      case x: Any => x != null
      case null => false
    }

  }

  def inc(v: Long, step: Long = 1): Long = v + step

  def reload(propFile: String = propFile): Unit = {
    val prop = new Properties()
    prop.load(Source.fromFile(propFile).reader())
    this.prop = prop.asScala
  }

  def loopUpProp(key: String) = {
    prop.getOrElse(key, EMPTYSTR) match {
      case EMPTYSTR => throw new CdmException("No property Found for " + key + " . specify property correctly")
      case x => x
    }
  }

  def printConfig(): Unit = {
    outStream.println("******************************************************************************************")
    outStream.println("***************** !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! **********************")
    outStream.println("*****************          Config For HL7 Processing *************************************")
    prop.foreach({ case (k, v) => outStream.println("********* " + k + " :: " + v) })
    outStream.println("***************** !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! **********************")
    outStream.println("******************************************************************************************")
    outStream.println("*****************           Job Initialization Started           **************************")
    outStream.println("*******************************************************************************************")
  }

  @throws[SerializationException]
  def serialize(data: Serializable): Array[Byte] = ser(data)

  @throws[SerializationException]
  def deSerialize(data: Array[Byte]): Object = des(data)

  def registerHook(hook: Thread): Unit = rt.addShutdownHook(hook)


  def unregister(hook: Thread): Unit = rt.removeShutdownHook(hook)

  def runnable(action: => Unit): Runnable =
    new Runnable {
      def run() = action
    }

  def newThread(name: String, runnable: Runnable, daemon: Boolean = false): Thread = {
    val thread = new Thread(runnable, name)
    thread.setDaemon(daemon)
    thread.setUncaughtExceptionHandler(new UncaughtExceptionHandler {
      override def uncaughtException(t: Thread, e: Throwable): Unit = error("Unexpected exception in thread '" + t.getName, e)
    })
    thread
  }


  def newDaemonCachedThreadPool(id: String): ThreadPoolExecutor = {
    Executors.newCachedThreadPool(new Factory(id)).asInstanceOf[ThreadPoolExecutor]
  }

  def newDaemonScheduler(id: String): ScheduledExecutorService = {
    Executors.newSingleThreadScheduledExecutor(new Factory(id))
  }

  def tryAndLogErrorThr(fun: => Unit, reporter: (Throwable) => Unit): Boolean = {
    try {
      fun
      return true
    }
    catch {
      case t: Throwable => reporter(t)
    }
    false
  }

  def tryAndLogErrorMes(fun: => Unit, reporter: (String) => Unit): Boolean = {
    try {
      fun
      return true
    }
    catch {
      case t: Throwable => reporter(t.getMessage)
    }
    false
  }

  private class Factory(id: String) extends ThreadFactory {
    private val cnt = new AtomicInteger(0)

    override def newThread(r: Runnable): Thread = {
      val t = new Thread(r, id + "-" + host + "-" + cnt.incrementAndGet())
      t.setDaemon(true)
      t
    }
  }

}
