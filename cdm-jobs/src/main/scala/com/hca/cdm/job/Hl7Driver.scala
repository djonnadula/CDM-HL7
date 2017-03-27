package com.hca.cdm.job

import java.io.File
import java.util.Date
import com.hca.cdm._
import com.hca.cdm.log.Logg
import com.hca.cdm.notification.TaskState._
import com.hca.cdm.notification.{EVENT_TIME, sendMail => mail}
import com.hca.cdm.outStream.{println => console}
import org.apache.spark.launcher.SparkAppHandle.Listener
import org.apache.spark.launcher.SparkAppHandle.State._
import org.apache.spark.launcher.SparkLauncher._
import org.apache.spark.launcher.{SparkAppHandle, SparkLauncher}
import com.hca.cdm.hl7.constants.HL7Constants.{COMMA, COLON}
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}
import scala.io.Source
import java.io.InputStream
import java.lang.{ProcessBuilder => runScript}
import org.apache.log4j.PropertyConfigurator._

/**
  * Created by Devaraj Jonnadula on 8/23/2016.
  *
  * Driver Submits Job to the Spark and Monitors in case of State Changes
  */
object Hl7Driver extends App with Logg {

  args length match {
    case 1 =>
      configure(currThread.getContextClassLoader.getResource("cdm-log4j.properties"))
      reload(args(0))
      console("******************************************************************************************")
      console("***************** !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! **********************")
      console("***************** !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! **********************")
      console("**************************HCA CDM HL7 Processing System Initiated ************************")
      console("***************** !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! **********************")
      console("***************** !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! **********************")
    case _ =>
      console("******************************************************************************************")
      console("***************** !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! **********************")
      console("*****************          NOT FOUND - configuration file:          **********************")
      console("                                " + args + "        ")
      console("***************** !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! **********************")
      console("******************************************************************************************")
      console("*****************                Job terminated                ***********************")
      console("*******************************************************************************************")
      abend(1)
  }
  printConfig()

  outStream.println(
    """Driver Initialised for ::
      ____
    HL / /    DATA
      / /
     / /
    /_/
    """)

  // ******************************************************** Spark Part ***********************************************
  private val app = lookUpProp("hl7.app")
  private val configFiles = new File(lookUpProp("hl7.config.files")) listFiles
  private val hl7_spark_deploy_mode = lookUpProp("hl7.spark.deploy-mode")
  private val defaultPar = lookUpProp("hl7.spark.default.parallelism")
  private val hl7_spark_executor_memory = lookUpProp("hl7.spark.executor-memory")
  private val offHeap = ((hl7_spark_executor_memory.substring(0, hl7_spark_executor_memory.indexOf("G")).toInt * 1024) / 10).toString
  private val hl7_spark_driver_maxResultSize = lookUpProp("hl7.spark.driver.maxResultSize")
  private val hl7_spark_dynamicAllocation_enabled = lookUpProp("hl7.spark.dynamicAllocation.enabled")
  private val hl7_spark_dynamicAllocation_minExecutors = lookUpProp("hl7.spark.dynamicAllocation.minExecutors")
  private val hl7_spark_dynamicAllocation_maxExecutors = lookUpProp("hl7.spark.dynamicAllocation.maxExecutors")
  private val hl7_spark_queue = lookUpProp("hl7.spark.queue")
  private val hl7_spark_master = lookUpProp("hl7.spark.master")
  private val hl7_spark_num_executors = lookUpProp("hl7.spark.num-executors")
  private val hl7_spark_driver_memory = lookUpProp("hl7.spark.driver-memory")
  private val ENV = lookUpProp("hl7.env")
  private val jobScript = lookUpProp("hl7.runner")
  private var sHook: Thread = _
  private val sparkLauncher = new SparkLauncher()
    .setAppName(app)
    .setMaster(hl7_spark_master)
    .setDeployMode(hl7_spark_deploy_mode)
    .setVerbose(true)
    .setConf(EXECUTOR_MEMORY, hl7_spark_executor_memory)
    .setConf(EXECUTOR_CORES, defaultPar)
    .setConf(DRIVER_MEMORY, hl7_spark_driver_memory)
    .setConf(CHILD_PROCESS_LOGGER_NAME, s"$app-Driver")
    .setConf("spark.driver.cores", "2")
    .setConf("spark.num-executors", hl7_spark_num_executors)
    .setConf("spark.dynamicAllocation.initialExecutors", hl7_spark_num_executors)
    .setConf("spark.yarn.queue", hl7_spark_queue)
    .setConf("spark.dynamicAllocation.enabled", hl7_spark_dynamicAllocation_enabled)
    .setConf("spark.dynamicAllocation.maxExecutors", hl7_spark_dynamicAllocation_maxExecutors)
    .setConf("spark.dynamicAllocation.minExecutors", hl7_spark_dynamicAllocation_minExecutors)
    .setConf("spark.driver.maxResultSize", hl7_spark_driver_maxResultSize)
    .setConf("spark.streaming.stopGracefullyOnShutdown", "true")
    .setConf("spark.yarn.max.executor.failures", "10")
    .setConf("spark.task.maxFailures", "10")
    .setMainClass(lookUpProp("hl7.class"))
    .setAppResource(lookUpProp("hl7.artifact"))
    .setJavaHome("/usr/bin/java")
    .setSparkHome(lookUpProp("spark.home"))
    .setConf("spark.yarn.preserve.staging.files", "true")
    .setConf("spark.ui.showConsoleProgress", "false")
    .setConf("spark.logConf", "true")
    .setConf("spark.eventLog.enabled", "true")
    .setConf("spark.ui.killEnabled", "false")
    .setConf("spark.serializer.objectStreamReset", "100")
    .setConf("spark.cleaner.ttl", "3600")
    .setConf("spark.dynamicAllocation.cachedExecutorIdleTimeout", "3600")
    .setConf("spark.yarn.executor.memoryOverhead", "2048")
    .setConf("spark.yarn.driver.memoryOverhead", "2048")
    .setConf("spark.ui.retainedJobs", "50")
    .setConf("spark.ui.retainedStages", "50")
    .setConf("spark.worker.ui.retainedExecutors", "50")
    .setConf("spark.worker.ui.retainedDrivers", "50")
    .setConf("spark.streaming.ui.retainedBatches", "50")
    .setConf("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .setConf("spark.yarn.keytab", lookUpProp("hl7.spark.yarn.keytab"))
    .setConf("spark.yarn.principal", lookUpProp("hl7.spark.yarn.principal"))
    .setConf("spark.streaming.stopSparkContextByDefault", "false")
    .setConf("spark.streaming.gracefulStopTimeout", "400000")
    .setConf("spark.streaming.concurrentJobs", lookUpProp("hl7.con.jobs"))
    .setConf("spark.hadoop.fs.hdfs.impl.disable.cache", lookUpProp("spark.hdfs.cache"))
  private lazy val extraConfig = () => lookUpProp("spark.extra.config")
  tryAndReturnDefaultValue(extraConfig, EMPTYSTR).split(COMMA, -1).foreach(conf => {
    if (valid(conf)) {
      val actConf = conf.split(COLON)
      if (valid(actConf, 2)) sparkLauncher.setConf(actConf(0), actConf(1))
    }
  })
  if (ENV != "PROD") {
    sparkLauncher.setConf("spark.driver.extraJavaOptions", s"-XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintGCTimeStamps -Dapp.logging.name=$app")
      .setConf("spark.executor.extraJavaOptions", s"-XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintGCTimeStamps -Dapp.logging.name=$app")
  } else {
    sparkLauncher.setConf("spark.driver.extraJavaOptions", s"-Dapp.logging.name=$app")
      .setConf("spark.executor.extraJavaOptions", s"-Dapp.logging.name=$app")
  }
  val configFile = new File(args(0))
  sparkLauncher addAppArgs configFile.getName
  sparkLauncher addFile configFile.getPath
  if (configFiles nonEmpty) configFiles foreach (file => sparkLauncher addFile (file getPath))
  private val startTime = currMillis
  private var watchTime = currMillis
  private val maxWait = 300000
  private val job = sparkLauncher startApplication()
  while (job.getAppId == null) {
    sleep(3000)
  }
  var check = false
  while (!check) {
    job getState match {
      case UNKNOWN => debug(app + " Job State Still Unknown ... ")
      case CONNECTED => info(app + " Job Connected ... Waiting for Resource Manager to Handle ...  " + job.getAppId)
      case SUBMITTED => info(app + " Job Submitted To Resource manager ... with Job ID :: " + job.getAppId)
      case RUNNING => check = true
        info(app + " Job Running ... with Job ID :: " + job.getAppId)
      case FINISHED | FAILED | KILLED =>
        error(app + " Job Something Wrong ... with Job ID :: " + job.getAppId)
        mail(app + " Job ID " + job.getAppId + " Current State " + job.getState,
          app + " Job in Critical State. This Should Not Happen for this Application. Some one has to Check What is happening with Job ID :: " + job.getAppId + " \n\n" + EVENT_TIME
          , CRITICAL)
        shutDown()
        startIfNeeded(lookUpProp("hl7.selfStart") toBoolean)
    }
    if ((currMillis - watchTime) >= maxWait) {
      mail(app + " Job ID " + job.getAppId + " Current State " + job.getState,
        app + " Job was submitted to Resource manager but not yet started Running. This Should Not Happen. Job Submitted since :: " + new Date(startTime).toString
          + " There might be some issue with Resource Manager and " +
          "Some one has to Check What is happening with Job ID :: " + job.getAppId + " \n\n" + EVENT_TIME
        , CRITICAL)
      watchTime = currMillis
    }
    sleep(1000)
  }
  watchTime = currMillis
  job addListener Tracker
  info(s"Registered Job Tracker $Tracker to Monitor $app")
  sHook = newThread(app + " Driver SHook", runnable({
    info(app + " Driver Shutdown Hook Called ")
    shutDown()
    handleDriver("stop")
    info(s"$app Driver Shutdown Completed ")
  }))
  registerHook(sHook)
  while (check) {
    sleep(600000)
    debug(app + " Job with Job Id : " + job.getAppId + " Running State ... " + job.getState)
  }

  private[job] object Tracker extends Listener {
    override def infoChanged(handle: SparkAppHandle): Unit = {
      // Not Needed
      // checkJob(handle)
      info(s"$app Info Changed with State ${handle.getState}")
    }

    override def stateChanged(handle: SparkAppHandle): Unit = checkJob(handle)

    private def checkJob(jobHandle: SparkAppHandle): Unit = {
      jobHandle getState match {
        case UNKNOWN => error(app + " Job  Disconnected From Cluster with state Unknown for Job Id ::  " + jobHandle.getAppId)
          mail("{encrypt} " + app + " Job ID " + jobHandle.getAppId + " Current State " + jobHandle.getState,
            app + "  Job Disconnected From Cluster with state Unknown. Monitor This Job and watch " +
              "few Minutes Later and Escalate it. \n \n Job ID :: " + job.getAppId + " \n\n Event Triggered time " + new Date().toString
            , WARNING)
        case CONNECTED => info(app + " Job  Connected  Back To Cluster ... Waiting for Resource Manager to Handle ...  " + jobHandle.getAppId)
        case SUBMITTED => info(app + " Job Submitted Back To Resource manager ... with Job ID :: " + jobHandle.getAppId)
          mail("{encrypt} " + app + " Job ID " + job.getAppId + " Current State " + jobHandle.getState,
            app + "  Job Submitted Back To Resource manager ... with Job ID :: " + job.getAppId + " Monitor Whether Job is Running or Not  \n\n" + EVENT_TIME
            , WARNING)
        case RUNNING =>
          info(app + " Job Running ... with Job ID :: " + jobHandle.getAppId + " from " + this)
        case FINISHED | FAILED =>
          error(s"$app Job ${jobHandle.getState} ... with Job ID ::  ${jobHandle.getAppId}")
          mail("{encrypt} " + app + " Job ID " + job.getAppId + " Current State " + jobHandle.getState,
            app + " Job in Critical State. This Should Not Happen for this Application. Some one has to Check What is happening with Job ID :: " + job.getAppId +
              "\n" + (if (lookUpProp("hl7.selfStart") toBoolean) s".Self Start is Requested. Will Make an Attempt To Start $app" else s".Self Start is not Enabled for $app . Exiting Job.") +
              " \n\n" + EVENT_TIME
            , CRITICAL)
          unregister(sHook)
          sHook.interrupt()
          shutDown()
          startIfNeeded(lookUpProp("hl7.selfStart") toBoolean)
        case KILLED =>
          error(s"$app Job ${jobHandle.getState} ... with Job ID ::  ${jobHandle.getAppId}")
          mail("{encrypt} " + app + " Job ID " + job.getAppId + " Current State " + jobHandle.getState,
            app + " Job is Killed. If job brought down for maintenance please ignore, other wise some one has to Check What is happening with Job ID :: " + job.getAppId + " \n\n" + EVENT_TIME
            , CRITICAL)
          unregister(sHook)
          sHook.interrupt()
          shutDown()
          handleDriver("stop")
      }
    }
  }

  private def shutDown(): Unit = {
    tryAndLogErrorMes(job stop(), error(_: Throwable))
    tryAndLogErrorMes(job kill(), error(_: Throwable))
    Try(runTime.exec(s"yarn application -kill ${job.getAppId}")) match {
      case Success(x) =>
        if (x.waitFor() != 0) {
          info(s"Stopping Job $app From Resource Manager resulted with Status ${getStatus(x.getInputStream)}. Kill Job Manually by yarn application -kill ${job.getAppId}")
        } else info(s"Stopped $app from Resource Manager with Status ${getStatus(x.getInputStream)}")
      case Failure(t) =>
        error(s"Stopping Job $app From Resource Manager failed with error ${t.getMessage}. Kill Job Manually by yarn application -kill ${job.getAppId}")
    }
  }

  private def startIfNeeded(selfStart: Boolean): Unit = {
    if (selfStart) {
      info(s"Self Start is Requested for $app")
      handleDriver("restart")
    } else {
      info(s"No Self Start is Requested So shutting down $app ...")
      handleDriver("stop")
      abend(0)
    }
  }

  private def handleDriver(command: String): Unit = {
    info(s"Trying to ${command.toUpperCase} Driver for $app by Running Script $jobScript $command")
    val process = new runScript(jobScript, command)
    process inheritIO()
    Try(process start) match {
      case Success(x) =>
        if (x.waitFor() != 0) error(s"${command.toUpperCase} Driver Process for $app failed with Status ${getStatus(x.getInputStream)}. Try Manually by $jobScript $command")
        else info(s"$app Driver Script ${command.toUpperCase} successfully ${Source.fromInputStream(x.getInputStream).getLines().mkString(COMMA)}")
      case Failure(t) =>
        error(s"${command.toUpperCase} Job $app failed. Try Manually by $jobScript $command", t)
    }
  }

  private def getStatus(stream: InputStream) = Source.fromInputStream(stream).getLines().mkString(COMMA)

}
