package com.hca.cdm.job.fieldops

import java.io.{File}
import java.lang.{ProcessBuilder => runScript}

import com.hca.cdm._
import com.hca.cdm.log.Logg
import com.hca.cdm.outStream.{println => console}
import org.apache.log4j.PropertyConfigurator._
import org.apache.spark.launcher.SparkLauncher._
import org.apache.spark.launcher.SparkLauncher

import scala.language.postfixOps

/**
  * Created by Peter James on 6/7/2017.
  *
  * Driver Submits Job to the Spark and Monitors in case of State Changes
  */
object EastFloridaMedAdminDriver extends App with Logg {

  args length match {
    case 1 =>
      configure(currThread.getContextClassLoader.getResource("cdm-log4j.properties"))
      reload(args(0))
      console("******************************************************************************************")
      console("***************** !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! **********************")
      console("***************** !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! **********************")
      console("************************** East Florida Medication Admin Job ************************")
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

  // ******************************************************** Spark Part ***********************************************
  private val app = lookUpProp("EFMedAdmin.app")
  private val efmedadmin_spark_deploy_mode = lookUpProp("EFMedAdmin.spark.deploy-mode")
  private val defaultPar = lookUpProp("EFMedAdmin.spark.default.parallelism")
  private val efmedadmin_spark_executor_memory = lookUpProp("EFMedAdmin.spark.executor-memory")
  private val offHeap = ((efmedadmin_spark_executor_memory.substring(0, efmedadmin_spark_executor_memory.indexOf("G")).toInt * 1024) / 10).toString
  private val efmedadmin_spark_driver_maxResultSize = lookUpProp("EFMedAdmin.spark.driver.maxResultSize")
  private val efmedadmin_spark_dynamicAllocation_enabled = lookUpProp("EFMedAdmin.spark.dynamicAllocation.enabled")
  private val efmedadmin_spark_dynamicAllocation_minExecutors = lookUpProp("EFMedAdmin.spark.dynamicAllocation.minExecutors")
  private val efmedadmin_spark_dynamicAllocation_maxExecutors = lookUpProp("EFMedAdmin.spark.dynamicAllocation.maxExecutors")
  private val efmedadmin_spark_queue = lookUpProp("EFMedAdmin.spark.queue")
  private val efmedadmin_spark_master = lookUpProp("EFMedAdmin.spark.master")
  private val efmedadmin_spark_num_executors = lookUpProp("EFMedAdmin.spark.num-executors")
  private val efmedadmin_spark_driver_memory = lookUpProp("EFMedAdmin.spark.driver-memory")
  private val ENV = lookUpProp("EFMedAdmin.env")
  private val jobScript = lookUpProp("EFMedAdmin.runner")
  private var sHook: Thread = _
  private val sparkLauncher = new SparkLauncher()
    .setAppName(app)
    .setMaster(efmedadmin_spark_master)
    .setDeployMode(efmedadmin_spark_deploy_mode)
    .setVerbose(true)
    .setConf(EXECUTOR_MEMORY, efmedadmin_spark_executor_memory)
    .setConf(EXECUTOR_CORES, defaultPar)
    .setConf(DRIVER_MEMORY, efmedadmin_spark_driver_memory)
    .setConf(CHILD_PROCESS_LOGGER_NAME, s"$app-Driver")
    .setConf("spark.driver.cores", "2")
    .setConf("spark.num-executors", efmedadmin_spark_num_executors)
    .setConf("spark.dynamicAllocation.initialExecutors", efmedadmin_spark_num_executors)
    .setConf("spark.yarn.queue", efmedadmin_spark_queue)
    .setConf("spark.dynamicAllocation.enabled", efmedadmin_spark_dynamicAllocation_enabled)
    .setConf("spark.dynamicAllocation.maxExecutors", efmedadmin_spark_dynamicAllocation_maxExecutors)
    .setConf("spark.dynamicAllocation.minExecutors", efmedadmin_spark_dynamicAllocation_minExecutors)
    .setConf("spark.driver.maxResultSize", efmedadmin_spark_driver_maxResultSize)
    .setConf("spark.streaming.stopGracefullyOnShutdown", "true")
    .setConf("spark.yarn.max.executor.failures", "10")
    .setConf("spark.task.maxFailures", "10")
    .setMainClass(lookUpProp("EFMedAdmin.class"))
    .setAppResource(lookUpProp("EFMedAdmin.artifact"))
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
    .setConf("spark.yarn.keytab", lookUpProp("EFMedAdmin.spark.yarn.keytab"))
    .setConf("spark.yarn.principal", lookUpProp("EFMedAdmin.spark.yarn.principal"))
    .setConf("spark.streaming.stopSparkContextByDefault", "false")
    .setConf("spark.streaming.gracefulStopTimeout", "400000")
    .setConf("spark.streaming.concurrentJobs", lookUpProp("EFMedAdmin.con.jobs"))
    .setConf("spark.hadoop.fs.hdfs.impl.disable.cache", lookUpProp("spark.hdfs.cache"))

  if (ENV != "PROD") {
    sparkLauncher.setConf("spark.driver.extraJavaOptions", s"-XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintGCTimeStamps -Dapp.logging.name=$app")
      .setConf("spark.executor.extraJavaOptions", s"-XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintGCTimeStamps -Dapp.logging.name=$app")
  } else {
    sparkLauncher.setConf("spark.driver.extraJavaOptions", s"-Dapp.logging.name=$app")
      .setConf("spark.executor.extraJavaOptions", s"-Dapp.logging.name=$app")
  }
  val configFile = new File(args(0))
  console("configFile: " + configFile.toString)
  sparkLauncher addAppArgs configFile.getName
  sparkLauncher addFile configFile.getPath
  sparkLauncher startApplication()
  sparkLauncher.launch()
  sHook = newThread(app + " Driver SHook", runnable())
  registerHook(sHook)
  console("here")
//  private def shutDown(): Unit = {
//    if (!job.getState.isFinal) {
//      tryAndLogErrorMes(job stop(), error(_: Throwable))
//      tryAndLogErrorMes(job kill(), error(_: Throwable))
//      Try(runTime.exec(s"yarn application -kill ${job.getAppId}")) match {
//        case Success(x) =>
//          if (tryAndLogErrorMes(x.waitFor(2, TimeUnit.MINUTES), error(_: Throwable))) {
//            info(s"Stopping Job $app From Resource Manager resulted with Status ${getStatus(x.getInputStream)}. Kill Job Manually by yarn application -kill ${job.getAppId}")
//          } else info(s"Stopped $app from Resource Manager with Status ${getStatus(x.getInputStream)}")
//        case Failure(t) =>
//          error(s"Stopping Job $app From Resource Manager failed with error ${t.getMessage}. Kill Job Manually by yarn application -kill ${job.getAppId}")
//      }
//    }
//  }
//
//  private def startIfNeeded(selfStart: Boolean): Unit = {
//    if (selfStart) {
//      info(s"Self Start is Requested for $app")
//      handleDriver("restart")
//    } else {
//      info(s"No Self Start is Requested So shutting down $app ...")
//      handleDriver("stop")
//      abend(0)
//    }
//  }

//  private def handleDriver(command: String): Unit = {
//    info(s"Trying to ${command.toUpperCase} Driver for $app by Running Script $jobScript $command")
//    val process = new runScript(jobScript, command)
//    process inheritIO()
//    Try(process start) match {
//      case Success(x) =>
//        if (tryAndLogErrorMes(x.waitFor(2, TimeUnit.MINUTES), error(_: Throwable))) error(s"${command.toUpperCase} Driver Process for $app failed with Status ${getStatus(x.getInputStream)}. Try Manually by $jobScript $command")
//        else info(s"$app Driver Script ${command.toUpperCase} successfully ${Source.fromInputStream(x.getInputStream).getLines().mkString(COMMA)}")
//      case Failure(t) =>
//        error(s"${command.toUpperCase} Job $app failed. Try Manually by $jobScript $command", t)
//    }
//  }
//
//  private def getStatus(stream: InputStream) = Source.fromInputStream(stream).getLines().mkString(COMMA)

}
