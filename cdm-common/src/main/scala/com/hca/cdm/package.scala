package com.hca

import java.io.{File, InputStream, PrintStream, Serializable}
import java.lang.Runtime.{getRuntime => rt}
import java.lang.Thread.UncaughtExceptionHandler
import java.net.{InetAddress, URL, URLClassLoader}
import java.nio.charset.StandardCharsets
import java.time.{LocalDate, Period, ZoneId}
import java.util.{Properties, TimeZone}
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ScheduledExecutorService, ThreadFactory, ThreadPoolExecutor}
import java.util.concurrent.Executors._
import Thread._
import System._
import com.hca.cdm.notification.{EVENT_TIME, sendMail => mail}
import com.hca.cdm.exception.CdmException
import com.hca.cdm.log.Logg
import com.hca.cdm.notification.TaskState._
import org.apache.commons.lang.SerializationException
import org.apache.commons.lang3.SerializationUtils.{deserialize => des, serialize => ser}
import TimeZone._
import java.util.UUID.randomUUID
import scala.collection.JavaConverters._
import scala.io.Source
import scala.language.postfixOps
import scala.util.Random

/**
  * Created by Devaraj Jonnadula on 8/19/2016.
  *
  * Commonly Used Utilities
  */
package object cdm extends Logg {

  lazy val UTF8 = StandardCharsets.UTF_8
  lazy val ASCII = StandardCharsets.US_ASCII
  private var prop: scala.collection.mutable.Map[String, String] = _
  var propFile = "CDMHL7.properties"
  lazy val EMPTYSTR = ""
  lazy val AMPERSAND = "&"
  lazy val emptyArray = Array.empty[Any]
  lazy val FS: String = File separator
  lazy val outStream: PrintStream = System out
  private lazy val random = new Random()


  def randomString: String = randomUUID.toString

  def sys_timeZone: TimeZone = getTimeZone(ZoneId.systemDefault())

  def sys_ZoneId: ZoneId = sys_timeZone.toZoneId

  def defaultSleep(): Unit = sleep(8000)

  def host: String = InetAddress.getLocalHost.getHostName

  def currMillis: Long = currentTimeMillis

  def currNanos: Long = nanoTime

  def currThread: Thread = currentThread()

  def sleep(howLong: Long): Unit = {
    try Thread.sleep(howLong)
    catch {
      case in: InterruptedException => error(currThread.getName + " Sleep Interrupted :: " + in.getMessage, in)
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

  def IOCanHandle(data: AnyRef, threshold: Int): Boolean = {
    data match {
      case x: Array[AnyRef] => x.length <= threshold
      case x: String => x.length <= threshold
      case _ => false
    }
  }

  def inc(v: Long, step: Long = 1): Long = v + step

  def reload(propFile: String = propFile, stream: Option[InputStream] = None): Unit = {
    synchronized {
      info(s"Loading Property File :: $propFile")
      val prop = new Properties()
      stream match {
        case Some(x) => prop.load(x)
        case _ => prop.load(Source.fromFile(propFile).reader())
      }
      this.prop = prop.asScala
      info(s"Env on $host")
      sys.env.foreach({ case (k, v) => info(s"$k :: $v") })
    }
  }

  def lookUpProp(key: String): String = {
    if (prop == null) reload()
    if (!prop.isDefinedAt(key)) throw new CdmException("No property Found for " + key + "  specify property correctly")
    prop getOrElse(key, EMPTYSTR)
  }

  def isConfigDefined(key: String): Boolean = prop isDefinedAt key

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
  def deSerialize(data: AnyRef): Object = {
    data match {
      case x: Array[Byte] => des(x)
      case x: InputStream => des(x)
    }
  }

  def registerHook(hook: Thread): Unit = rt addShutdownHook hook


  def unregister(hook: Thread): Boolean = {
    try {
      rt removeShutdownHook hook
      hook interrupt()
      hook join()
      return true
    }
    catch {
      case ist: Throwable => error(s"Cannot Remove Shutdown hook :: ${hook.getName}", ist)
    }
    false
  }

  def runTime: Runtime = rt

  def runnable(action: => Unit): Runnable =
    new Runnable {
      def run(): Unit = action
    }

  def newThread(name: String, runnable: Runnable, daemon: Boolean = false): Thread = {
    val thread = new Thread(runnable, name)
    thread.setDaemon(daemon)
    thread.setUncaughtExceptionHandler(new UncaughtExceptionHandler {
      override def uncaughtException(t: Thread, e: Throwable): Unit =
        error(s"Unexpected exception in thread ' ${t.getName}", e)
    })
    thread
  }


  def newDaemonCachedThreadPool(id: String): ThreadPoolExecutor = {
    newCachedThreadPool(new Factory(id)).asInstanceOf[ThreadPoolExecutor]
  }

  def newDaemonScheduler(id: String): ScheduledExecutorService = {
    newSingleThreadScheduledExecutor(new Factory(id))
  }

  def tryAndLogThr(fun: => Unit, whichAction: String, reporter: (Throwable) => Unit, notify: Boolean = false, state: taskState = CRITICAL): Boolean = {
    try {
      fun
      return true
    }
    catch {
      case t: Throwable => reporter(t)
        if (notify) {
          mail("{encrypt} " + lookUpProp("hl7.app") + " Function Execution Failed due to  " + t.getMessage,
            " Executing Function Failed for " + whichAction + " due to Exception :: " + t.getClass +
              " & Stack Trace is as follows \n\n" + t.getStackTrace.mkString("\n") + "\n\n" + EVENT_TIME, state)
        }
    }
    false
  }

  def tryAndLogErrorMes(fun: => Unit, reporter: (Throwable) => Unit, message: Option[String] = None): Boolean = {
    try {
      fun
      return true
    }
    catch {
      case t: Throwable =>
        if (message isDefined) error(message.get, t)
        else reporter(t)
    }
    false
  }

  def tryAndThrow[T](fun: => T, reporter: (Throwable) => Unit, message: Option[String] = None): T = {
    try {
      val out = fun
      if (null != out) return out
    }
    catch {
      case t: Throwable =>
        reporter(t)
        throw new CdmException(message.getOrElse(EMPTYSTR), t)
    }
    null.asInstanceOf[T]
  }

  def tryAndLogErrorMes[T](fun: () => T, reporter: (String, Throwable) => Unit): Option[T] = {
    try {
      val out = fun()
      if (null != out) return Some(out)
    }
    catch {
      case t: Throwable => reporter(s"Cannot Execute Function ${t.getMessage}", t)
    }
    None
  }

  def tryAndReturnDefaultValue[T](fun: () => T, default: T): T = {
    val temp = tryAndLogErrorMes(fun, debug(_: String, _: Throwable))
    temp getOrElse default
  }

  def abend(code: Int = -1): Unit = System exit code

  def exists[T](store: Map[T, AnyRef], key: T): Boolean = store isDefinedAt key

  def enabled[T](key: T): Option[T] = {
    key match {
      case s: String => if (null != s && s != EMPTYSTR) return Some(s.asInstanceOf[T])
      case any => if (valid(any)) return Some(any.asInstanceOf[T])
    }
    None
  }

  def loadClass[T](clazz: String, specificJar: Option[String] = None): T = {
    if(specificJar isDefined) return new URLClassLoader(Array[URL](new URL("file:" + new File(specificJar.get).getAbsolutePath))).loadClass(clazz).newInstance().asInstanceOf[T]
    currThread.getContextClassLoader.loadClass(clazz).newInstance().asInstanceOf[T]
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
