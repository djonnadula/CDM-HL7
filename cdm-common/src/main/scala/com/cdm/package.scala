package com.hca

import java.io.{File, InputStream, PrintStream, Serializable}
import java.lang.Runtime.{getRuntime => rt}
import scala.reflect.runtime.universe._
import java.lang.Thread.UncaughtExceptionHandler
import java.net.{InetAddress, URL, URLClassLoader}
import java.nio.charset.StandardCharsets
import java.time.{LocalDate, Period, ZoneId}
import java.util.{Properties, TimeZone}
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent._
import java.util.concurrent.Executors._
import Thread._
import System._
import com.cdm.notification.{EVENT_TIME, sendMail => mail}
import com.cdm.exception.CdmException
import com.cdm.log.Logg
import com.cdm.notification.TaskState._
import org.apache.commons.lang.SerializationException
import org.apache.commons.lang3.SerializationUtils.{deserialize => des, serialize => ser}
import TimeZone._
import java.util.UUID.randomUUID
import scala.collection.JavaConverters._
import scala.io.{BufferedSource, Source}
import scala.io.Source.fromFile
import scala.language.{implicitConversions, postfixOps}
import scala.util.{Failure, Random, Success, Try}
import java.lang.{ProcessBuilder => runScript}

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
  lazy val SPACE = " "
  lazy val AMPERSAND = "&"
  lazy val emptyArray = Array.empty[Any]
  lazy val FS: String = File separator
  lazy val outStream: PrintStream = System out
  private lazy val random = new Random()
  private val CR: Char = 0x0D.toChar
  private val LF: Char = 0x0A.toChar
  lazy val CRLF = s"$CR$LF"
  private lazy val notifyErrors: Boolean = tryAndReturnDefaultValue(asFunc(lookUpProp("cdm.notify.errors").toBoolean), true)


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

  def getJar(fromLoc: String): Option[String] = {
    Try(fromLoc.substring(fromLoc.lastIndexOf(FS) + 1)).toOption
  }

  def closeResource(res: AutoCloseable): Unit = if (res != null) res.close()

  def valid(d: Any, minSize: Int = 1): Boolean = {
    d match {
      case x: Array[Any] => x != null & x.length >= minSize
      case x: String => x != null
      case x: Traversable[Any] => x != null & x.nonEmpty
      case x: Any => x != null
      case null | None => false
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

  def dec(v: Long, step: Long = 1): Long = v - step

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

  def readFile(file: String): BufferedSource = {
    if (lookUpProp("hl7.env") == "LOCAL") {
      new BufferedSource(currThread.getContextClassLoader.getResourceAsStream(file))
    } else fromFile(file)
  }


  def loadConfig(config_file: String): Properties = {
    val config = new Properties()
    config.load(readFile(config_file).reader())
    config
  }

  def lookUpProp(key: String): String = {
    if (prop == null) reload()
    if (!prop.isDefinedAt(key)) throw new CdmException("No property Found for " + key + "  specify property correctly")
    prop getOrElse(key, EMPTYSTR)
  }

  def isConfigDefined(key: String): Boolean = {
    if (prop == null) return false
    prop isDefinedAt key
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
  def deSerialize[T](data: AnyRef): T = {
    data match {
      case x: Array[Byte] => des(x).asInstanceOf[T]
      case x: InputStream => des(x).asInstanceOf[T]
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

  def newDaemonCachedThreadPool(id: String, poolSize: Int, IdleTimeHours: Long = Long.MaxValue): ThreadPoolExecutor = {
    new ThreadPoolExecutor(poolSize, Integer.MAX_VALUE, IdleTimeHours, TimeUnit.HOURS, new LinkedBlockingQueue[Runnable], new Factory(id))
  }

  def newDaemonScheduler(id: String): ScheduledExecutorService = {
    newSingleThreadScheduledExecutor(new Factory(id))
  }

  def newDaemonCachedScheduler(id: String,poolSize: Int): ScheduledExecutorService = {
    newScheduledThreadPool(poolSize,new Factory(id))
  }

  def tryAndLogThr(fun: => Unit, whichAction: String, reporter: (Throwable) => Unit, notify: Boolean = notifyErrors, state: taskState = CRITICAL): Boolean = {
    try {
      fun
      return true
    }
    catch {
      case t: Throwable => reporter(t)
        if (notify) {
          mail("{encrypt} " + lookUpProp("hl7.app") + " Function Execution Failed due to  " + (if (t.getCause != null) t.getCause.getMessage else t.getMessage),
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

  def tryAndReturnThrow[T](fun: => T): Either[T, Throwable] = {
    try {
      val out = fun
      Left(out)
    }
    catch {
      case t: Throwable =>
        Right(new CdmException(t))
    }
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
    tryAndLogErrorMes(fun, debug(_: String, _: Throwable)) getOrElse default
  }

  def tryAndReturnDefaultValue0[T](fun: => T, default: T): T = {
    tryAndReturnDefaultValue(asFunc(fun), default)
  }

  def tryAndFallbackTo[T](fun: () => T, fallbackTo: => T): T = {
    tryAndLogErrorMes(fun, debug(_: String, _: Throwable)) getOrElse tryAndThrow(fallbackTo, error(_: Throwable))
  }

  def tryAndGoNextAction[T](fun: () => T, nextStep: => Unit): T = {
    val opOut = tryAndLogErrorMes(fun, debug(_: String, _: Throwable))
    nextStep
    opOut.getOrElse(null.asInstanceOf[T])
  }

  def tryAndGoNextAction0[T](fun: => T, nextStep: => Unit): T = {
    tryAndGoNextAction(asFunc(fun), nextStep)
  }


  def asFunc[T](action: => T): () => T = () => action

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
    if (specificJar isDefined) return new URLClassLoader(Array[URL](new URL("file:" + new File(specificJar.get).getAbsolutePath)))
      .loadClass(clazz).newInstance().asInstanceOf[T]
    currThread.getContextClassLoader.loadClass(clazz).newInstance().asInstanceOf[T]
  }

  def typeFromClass[T](clazz: Class[T])(implicit mirror: Mirror): Type = mirror.classSymbol(clazz).toType

  def getOS: String = sys.env.getOrElse("os.name", EMPTYSTR)

  def trimStr(in: String): String = if (valid(in)) in.trim else EMPTYSTR

  def executeScript(commands: java.util.List[String], expectedTime: Int = 60): Boolean = {
    info(s"Executing Commands $commands")
    val process = new runScript(commands)
    process inheritIO()
    Try(process start) match {
      case Success(x) =>
        if (tryAndLogErrorMes(x.waitFor(expectedTime, TimeUnit.MINUTES), error(_: Throwable))) {
          info(s"Status for $commands ${Source.fromInputStream(x.getInputStream).getLines().mkString(",")}")
          info(s"Error Status for $commands ${Source.fromInputStream(x.getErrorStream).getLines().mkString(",")}")
          if (x.exitValue() != 0) return false
        }
      case Failure(t) =>
        error(s"Executing Commands Failed $commands", t)
        return false
    }
    true
  }


  private[cdm] class Factory(id: String, assignName: () => String = () => EMPTYSTR) extends ThreadFactory {
    private val cnt = new AtomicInteger(0)

    override def newThread(r: Runnable): Thread = {
      cdm.newThread(id + "-" + host + "-" + assignName() + "-" + cnt.incrementAndGet(), r, daemon = true)
    }
  }


}
