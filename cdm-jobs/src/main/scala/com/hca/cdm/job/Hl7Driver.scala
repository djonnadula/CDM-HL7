package com.hca.cdm.job


import java.util.TimerTask
import java.util.concurrent.TimeUnit

import com.hca.cdm._
import com.hca.cdm.hl7.constants.HL7Types.{withName => hl7}
import com.hca.cdm.kafka.config.HL7ConsumerConfig.{createConfig => conf}
import com.hca.cdm.kafka.producer.{KafkaProducerHandler => KProducer}
import com.hca.cdm.log.Logg
import com.hca.cdm.spark.config.{Hl7SparkUtil => sp}
import org.apache.spark.launcher.SparkAppHandle.State
import org.apache.spark.launcher.SparkLauncher
import org.apache.spark.launcher.SparkLauncher._

import scala.language.postfixOps

/**
  * Created by Devaraj Jonnadula on 8/23/2016.
  */
object Hl7Driver extends App with Logg {

  args length match {
    case 1 => reload(args(0))
      outStream.println("******************************************************************************************")
      outStream.println("***************** !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! **********************")
      outStream.println("***************** !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! **********************")
      outStream.println("**************************HCA CDM HL7 Processing System Initiated ************************")
      outStream.println("***************** !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! **********************")
      outStream.println("***************** !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! **********************")
    case _ =>
      outStream.println("******************************************************************************************")
      outStream.println("***************** !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! **********************")
      outStream.println("*****************          NOT FOUND - configuration file:          **********************")
      outStream.println("                                " + args.mkString(",") + "        ")
      outStream.println("***************** !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! **********************")
      outStream.println("******************************************************************************************")
      outStream.println("*****************                Program terminated                ***********************")
      outStream.println("*******************************************************************************************")
      System.exit(-8)
  }

  outStream.println(
    """Driver Initialised ::
      ____
    HL / /    DATA
      / /
     / /
    /_/
    """)
  printConfig()

  // ******************************************************** Spark Part ***********************************************
  private val hl7_spark_driver_maxResultSize = loopUpProp("hl7.spark.driver.maxResultSize")
  private val hl7_spark_dynamicAllocation_enabled = loopUpProp("hl7.spark.dynamicAllocation.enabled")
  private val hl7_spark_dynamicAllocation_minExecutors = loopUpProp("hl7.spark.dynamicAllocation.minExecutors")
  private val hl7_spark_dynamicAllocation_maxExecutors = loopUpProp("hl7.spark.dynamicAllocation.maxExecutors")
  private val hl7_spark_queue = loopUpProp("hl7.spark.queue")
  private val hl7_spark_master = loopUpProp("hl7.spark.master")
  private val hl7_spark_deploy_mode = loopUpProp("hl7.spark.deploy-mode")
  private val hl7_spark_num_executors = loopUpProp("hl7.spark.num-executors")
  private val hl7_spark_driver_memory = loopUpProp("hl7.spark.driver-memory")
  private val hl7_spark_executor_memory = loopUpProp("hl7.spark.executor-memory")

  // ******************************************************** JOB Part ***********************************************
  private val app = loopUpProp("hl7.app")
  private val defaultPar = loopUpProp("hl7.spark.default.parallelism")
  private val hl7MsgMeta = loopUpProp("hl7.messages.type").split(",").toList
  private val job = new SparkLauncher().setAppName(app)
    .setMaster(hl7_spark_master).setDeployMode(hl7_spark_deploy_mode).setVerbose(true).
    setConf(EXECUTOR_MEMORY, hl7_spark_executor_memory).setConf(EXECUTOR_CORES, defaultPar)
    .setConf("spark.driver-memory", hl7_spark_driver_memory)
    .setConf("spark.num-executors", hl7_spark_num_executors)
    .setConf("spark.queue", hl7_spark_queue)
    .setConf("spark.dynamicAllocation.maxExecutors", hl7_spark_dynamicAllocation_minExecutors)
    .setConf("spark.dynamicAllocation.minExecutors", hl7_spark_dynamicAllocation_minExecutors)
    .setConf("spark.driver.maxResultSize", hl7_spark_driver_maxResultSize)
    .setConf("spark.driver.maxResultSize", hl7_spark_driver_maxResultSize)
    .setMainClass(loopUpProp("hl7.class"))
    .setAppResource(loopUpProp("hl7.artifact")).setJavaHome("/usr/bin/java")
    .setSparkHome(loopUpProp("spark.home")).startApplication()
  private val jobTracker = newDaemonScheduler(getClass.getName)

  info(" Job Started with ID :: " + job.getAppId)
  registerHook(newThread(app + this.getClass.getName + " Driver SHook", runnable({
    job.stop()
    jobTracker.shutdown()
    info(currThread.getName + " Shutdown Completed for Driver")
  })))

  jobTracker.scheduleAtFixedRate(Tracker, 5, 5, TimeUnit.MILLISECONDS)
  var state = true
  while (state) {
    // info(" Job State   :: "+job.getState )
    if (job.getState == State.CONNECTED) {
      info(" Job Connected  now submitting :: " + job.getState)
      state = false
    }
  }

  object Tracker extends TimerTask {

    override def run(): Unit = {
      hl7MsgMeta.foreach(ty => info("Job Current State for Message Type :: " + ty + " :: " + job.getState))

    }
  }

}
