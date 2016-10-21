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
import scala.language.postfixOps

/**
  * Created by Devaraj Jonnadula on 8/23/2016.
  *
  * Driver Submits Job to the Spark and Monitors in case of State Changes
  */
object Hl7Driver extends App with Logg {

  args length match {
    case 1 => reload(args(0))
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
      System.exit(-8)
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
  private val hl7_spark_driver_maxResultSize = lookUpProp("hl7.spark.driver.maxResultSize")
  private val hl7_spark_dynamicAllocation_enabled = lookUpProp("hl7.spark.dynamicAllocation.enabled")
  private val hl7_spark_dynamicAllocation_minExecutors = lookUpProp("hl7.spark.dynamicAllocation.minExecutors")
  private val hl7_spark_dynamicAllocation_maxExecutors = lookUpProp("hl7.spark.dynamicAllocation.maxExecutors")
  private val hl7_spark_queue = lookUpProp("hl7.spark.queue")
  private val hl7_spark_master = lookUpProp("hl7.spark.master")
  private val hl7_spark_num_executors = lookUpProp("hl7.spark.num-executors")
  private val hl7_spark_driver_memory = lookUpProp("hl7.spark.driver-memory")
  private val sparkLauncher = new SparkLauncher()
    .setAppName(app)
    .setMaster(hl7_spark_master)
    .setDeployMode(hl7_spark_deploy_mode)
    .setVerbose(true)
    .setConf("spark.akka.frameSize", "200")
    .setConf(EXECUTOR_MEMORY, hl7_spark_executor_memory)
    .setConf(EXECUTOR_CORES, defaultPar)
    .setConf("spark.driver-memory", hl7_spark_driver_memory)
    .setConf("spark.num-executors", hl7_spark_num_executors)
    .setConf("spark.dynamicAllocation.initialExecutors", hl7_spark_num_executors)
    .setConf("spark.queue", hl7_spark_queue)
    .setConf("spark.dynamicAllocation.enabled", hl7_spark_dynamicAllocation_enabled)
    .setConf("spark.dynamicAllocation.maxExecutors", hl7_spark_dynamicAllocation_maxExecutors)
    .setConf("spark.dynamicAllocation.minExecutors", hl7_spark_dynamicAllocation_minExecutors)
    .setConf("spark.driver.maxResultSize", hl7_spark_driver_maxResultSize)
    .setMainClass(lookUpProp("hl7.class"))
    .setAppResource(lookUpProp("hl7.artifact"))
    .setJavaHome("/usr/bin/java")
    .setSparkHome(lookUpProp("spark.home"))
    .setConf("spark.yarn.preserve.staging.files", "true")
  val configFile = new File(args(0))
  sparkLauncher addAppArgs configFile.getName
  sparkLauncher addFile configFile.getPath
  configFiles foreach (file => sparkLauncher addFile (file getPath))
  private val startTime = currMillis
  private var watchTime = currMillis
  private val maxWait = 300000
  private val job = sparkLauncher startApplication()
  registerHook(newThread(app + " Driver SHook", runnable({
    info(app + " Driver Shutdown Hook Called ")
    job stop()
    job kill()
    info(app + " Driver Shutdown Completed ")
  })))

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
          app + " Job in Critical State. This Should Not Happen for this Application. Some one has to Check What was happening with Job ID :: " + job.getAppId + " \n\n" + EVENT_TIME
          , CRITICAL)
        abend(1)
    }
    if ((currMillis - watchTime) >= maxWait) {
      mail(app + " Job ID " + job.getAppId + " Current State " + job.getState,
        app + " Job was submitted to Resource manager but not yet started Running. This Should Not Happen. Job Submitted since :: " + new Date(startTime).toString
          + " There might be some issue with Resource Manager and " +
          "Some one has to Check What was happening with Job ID :: " + job.getAppId + " \n\n" + EVENT_TIME
        , CRITICAL)
      watchTime = currMillis
    }
    sleep(1000)
  }
  watchTime = currMillis
  job addListener Tracker
  while (check) {
    sleep(600000)
    trace(app + " Job with Job Id : " + job.getAppId + " Running State ... " + job.getState)
  }

  object Tracker extends Listener {
    override def infoChanged(handle: SparkAppHandle): Unit = checkJob(handle)

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
        case RUNNING => info(app + " Job Running ... with Job ID :: " + jobHandle.getAppId)
        case FINISHED | FAILED | KILLED =>
          error(app + " Job Something Wrong ... with Job ID :: " + jobHandle.getAppId)
          mail("{encrypt} " + app + " Job ID " + job.getAppId + " Current State " + jobHandle.getState,
            app + " Job in Critical State. This Should Not Happen for this Application. Some one has to Check What was happening with Job ID :: " + job.getAppId + " \n\n" + EVENT_TIME
            , CRITICAL)
          abend(1)
      }
    }
  }

}
