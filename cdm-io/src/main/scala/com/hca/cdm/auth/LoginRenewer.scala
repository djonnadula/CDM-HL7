package com.hca.cdm.auth

import java.util.concurrent.TimeUnit
import com.hca.cdm._
import com.hca.cdm.log.Logg
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.security.{UserGroupInformation => UGI, _}
import org.apache.spark.deploy.yarn.YarnSparkHadoopUtil.{get => yrnUtil}
import scala.language.postfixOps
import org.apache.spark.deploy.SparkHadoopUtil.{get => hdpUtil}
import TimeUnit.MINUTES.{toMillis => fromMinutes}
import TimeUnit.HOURS.{toMillis => fromHours}
import java.security.PrivilegedExceptionAction
import com.hca.cdm.exception.CdmException
import org.apache.spark.SparkConf

/**
  * Created by Devaraj Jonnadula on 2/15/2017.
  */
private[cdm] object LoginRenewer extends Logg {

  private lazy val app = lookUpProp("hl7.app")
  private lazy val loginRenewer = newDaemonScheduler(s"$app-Login-Renewer")
  private var hdfsConf: Configuration = _
  private var fs: FileSystem = _
  private var scheduled = false
  private val lock = new Object

  def isSecured: Boolean = UGI.isSecurityEnabled

  def scheduleLoginFromCredentials(startFrom: Int = 6, credentialsFile: String, stagingDIr: String): Unit = {
    lock.synchronized {
      if (!scheduled) {
        info(s"Credentials File set to $credentialsFile and Staging Dir $stagingDIr")
        if (isSecured) {
          hdfsConf = byPassConfig(new Path(credentialsFile).toUri.getScheme, hdpUtil.conf)
          fs = FileSystem.get(hdfsConf)
          loginRenewer scheduleWithFixedDelay(runnable(tryAndLogErrorMes(accessCredentials(stagingDIr + FS + credentialsFile), error(_: Throwable))
          ), startFrom, startFrom, TimeUnit.HOURS)
          sHook
          scheduled = true
        }
      }
    }
  }

  def loginFromKeyTab(keyTab: String, principal: String, config: Option[Configuration]): Boolean = {
    config.foreach(cfg => UGI.setConfiguration(cfg))
    val currUser = UGI.loginUserFromKeytabAndReturnUGI(principal, keyTab)
    if (currUser != null) return true
    false
  }

  def scheduleGenCredentials(startFrom: Int = 1, credentialsFile: Path, principal: String, keytab: String, nns: Set[Path]): Unit = {
    loginRenewer scheduleWithFixedDelay(runnable(tryAndLogErrorMes(genCredentials(credentialsFile, principal, keytab, nns), error(_: Throwable)))
      , startFrom, startFrom, TimeUnit.MINUTES)
    sHook
  }

  def credentialFile(stagingDir: String, suffix: String = "Credentials"): Path = {
    UGI.getCurrentUser.doAs(new PrivilegedExceptionAction[Path] {
      override def run(): Path = {
        val conf = hdpUtil.conf
        fs = FileSystem.get(conf)
        val crPath = new Path(s"$stagingDir$FS$app-$suffix")
        fs.createNewFile(crPath)
        hdfsConf = byPassConfig(crPath.toUri.getScheme, hdpUtil.conf)
        fs = FileSystem.get(hdfsConf)
        crPath
      }
    })
  }

  def haNameNodes(conf: SparkConf): Set[Path] = {
    yrnUtil.getNameNodesToAccess(conf)
  }

  def stop(): Unit = {
    loginRenewer shutdown()
    loginRenewer awaitTermination(1, TimeUnit.MINUTES)
    info(s"$app-Login-Renewer Shutdown Completed")
  }

  @throws[CdmException]
  def performAction[T](fun: () => T): T = {
    tryAndThrow({
      UGI.getCurrentUser.doAs(new PrivilegedExceptionAction[T] {
        override def run(): T = fun()
      })
    }, error(_: Throwable))
  }

  private def byPassConfig(scheme: String, conf: Configuration): Configuration = {
    val key = s"fs.$scheme.impl.disable.cache"
    val tempConf = new Configuration(conf)
    tempConf.setBoolean(key, true)
    tempConf
  }

  private def refreshFsTokens(nameNodes: Set[Path], credentials: Credentials): Unit = {
    info("Renewer for Credentials " + yrnUtil.getTokenRenewer(hdfsConf))
    nameNodes.foreach(node => {
      val dfs = node.getFileSystem(hdfsConf)
      try {
        info(s" Refreshed Tokens for File System ${dfs.addDelegationTokens(yrnUtil.getTokenRenewer(hdfsConf), credentials)}")
      }
      catch {
        case t: Throwable => error(s"cannot Refresh Tokens for $node & FileSystem $dfs", t)
      }
    })
  }

  private def accessCredentials(credentialFile: String): Unit = {
    val cred = new Credentials()
    cred.readTokenStorageStream(fs.open(new Path(credentialFile)))
    info(s"Before Credentials Update ${UGI.getCurrentUser.getCredentials.getAllTokens}")
    info(s"Credentials found ${cred.getAllTokens}")
    UGI.getCurrentUser.addCredentials(cred)
    info(s"After Credentials Update ${UGI.getCurrentUser.getCredentials.getAllTokens}")
  }

  private def genCredentials(credentialsFile: Path, principal: String, keytab: String, nns: Set[Path]): Unit = {
    val curUser = UGI.getCurrentUser
    val loggedUser = curUser.doAs(new PrivilegedExceptionAction[UGI] {
      override def run(): UGI = {
        UGI.loginUserFromKeytabAndReturnUGI(principal, keytab)
      }
    })
    val cred = loggedUser.getCredentials
    loggedUser.doAs(new PrivilegedExceptionAction[Void] {
      override def run(): Void = {
        refreshFsTokens(nns + credentialsFile.getParent, cred)
        cred.writeTokenStorageFile(credentialsFile, hdfsConf)
        UGI.setLoginUser(loggedUser)
        null
      }
    })
  }
  private def sHook : Unit= registerHook(newThread(s"$app-Login-Renewer-SHook", runnable(stop())))

}






