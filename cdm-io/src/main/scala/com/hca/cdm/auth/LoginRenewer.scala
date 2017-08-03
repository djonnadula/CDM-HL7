package com.hca.cdm.auth

import java.io.{DataInputStream, InputStream}
import java.util.concurrent.TimeUnit._
import com.hca.cdm.{lookUpProp, _}
import com.hca.cdm.log.Logg
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.security.{UserGroupInformation => UGI, _}
import org.apache.spark.deploy.yarn.YarnSparkHadoopUtil.{get => yrnUtil}
import java.lang.System.{getenv => fromEnv}
import scala.language.postfixOps
import org.apache.spark.deploy.SparkHadoopUtil.{get => hdpUtil}
import java.security.PrivilegedExceptionAction
import com.hca.cdm.exception.CdmException
import org.apache.hadoop.mapred.Master
import org.apache.spark.SparkConf

/**
  * Created by Devaraj Jonnadula on 2/15/2017.
  */
private[cdm] object LoginRenewer extends Logg {

  private lazy val app = "HDFS"
  private lazy val loginRenewer = newDaemonScheduler(s"$app-Token-Renewer")
  private lazy val tryRenewal = if (isConfigDefined("hl7.hdfs.token.renewal")) lookUpProp("hl7.hdfs.token.renewal").toBoolean else true
  private var hdfsConf: Configuration = _
  private var fs: FileSystem = _
  private var scheduled = false
  private val lock = new Object

  def scheduleRenewal(master: Boolean = false, namesNodes: String = EMPTYSTR): Boolean = synchronized {
    if (!scheduled && isSecured) {
      val appHomeDir = FileSystem.get(new Configuration()).getHomeDirectory
      val tempCrd = credentialFile(s"$appHomeDir$FS${fromEnv("SPARK_YARN_STAGING_DIR")}")
      val sparkConf = new SparkConf
      if (!master) sparkConf.set("spark.yarn.access.namenodes", if (namesNodes == EMPTYSTR) lookUpProp("secure.name.nodes") else namesNodes)
      if (master) scheduleGenCredentials(2, tempCrd, sparkConf.get("spark.yarn.principal"), sparkConf.get("spark.yarn.keytab"), haNameNodes(sparkConf))
      scheduleLoginFromCredentials(2.1.toLong, tempCrd.getName, fromEnv("SPARK_YARN_STAGING_DIR"))
    }
    scheduled
  }

  def isSecured: Boolean = UGI.isSecurityEnabled && tryRenewal

  private def scheduleLoginFromCredentials(startFrom: Long = 6, credentialsFile: String, stagingDIr: String): Unit = {
    lock.synchronized {
      if (!scheduled) {
        info(s"Credentials File set to $credentialsFile and Staging Dir $stagingDIr")
        hdfsConf = byPassConfig(new Path(credentialsFile).toUri.getScheme, hdpUtil.conf)
        hdfsConf.set("hadoop.security.authentication", "Kerberos")
        fs = FileSystem.get(hdfsConf)
        loginRenewer scheduleAtFixedRate(runnable(tryAndLogErrorMes(accessCredentials(stagingDIr + FS + credentialsFile), error(_: Throwable))
        ), startFrom, MILLISECONDS.convert(startFrom, HOURS), MILLISECONDS)
        sHook()
        scheduled = true
      }
    }
  }

  def loginFromKeyTab(keyTab: String, principal: String, config: Option[Configuration]): Boolean = {
    info(s"Logging $principal with KeyTab $keyTab")
    config.foreach(cfg => UGI.setConfiguration(cfg))
    val currUser = UGI.loginUserFromKeytabAndReturnUGI(principal, keyTab)
    UGI.getCurrentUser.addCredentials(currUser.getCredentials)
    if (currUser != null) return true
    false
  }

  def scheduleGenCredentials(startFrom: Long = 1, credentialsFile: Path, principal: String, keytab: String, nns: Set[Path]): Unit = {
    loginRenewer scheduleAtFixedRate(runnable(tryAndLogErrorMes(genCredentials(credentialsFile, principal, keytab, nns), error(_: Throwable)))
      , startFrom, MILLISECONDS.convert(startFrom, HOURS), MILLISECONDS)
    sHook()
  }

  private def credentialFile(stagingDir: String, suffix: String = s"Credentials"): Path = {
        val conf = hdpUtil.conf
        fs = FileSystem.get(conf)
        val crPath = new Path(s"$stagingDir$FS$app-$suffix")
        fs.createNewFile(crPath)
        hdfsConf = byPassConfig(crPath.toUri.getScheme, hdpUtil.conf)
        fs = FileSystem.get(hdfsConf)
        crPath
  }

  private def haNameNodes(conf: SparkConf): Set[Path] = {
    yrnUtil.getNameNodesToAccess(conf)
  }

  def stop(): Unit = {
    loginRenewer shutdown()
    loginRenewer awaitTermination(1, MINUTES)
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
    val renewer = yrnUtil.getTokenRenewer(hdfsConf)
    //Master.getMasterPrincipal(hdfsConf)
    info("Renewer for Credentials " + renewer)
    nameNodes.foreach(node => {
      val dfs = node.getFileSystem(hdfsConf)
      try {
        val token = dfs.addDelegationTokens(renewer, credentials)
        info(s"Refreshed Tokens for File System $node ")
        token.foreach { tkn => info(s"Refreshed Tokens ${tkn.getService} ${tkn.getKind} ${tkn.getIdentifier.mkString}") }
      }
      catch {
        case t: Throwable => debug(s"cannot Refresh Tokens for $node & FileSystem $dfs", t)
      }
    })
  }

  private def accessCredentials(credentialFile: String): Unit = {
    val credentialPath = new Path(credentialFile)
    var stream: DataInputStream = null
    if (fs.exists(credentialPath) && fs.getFileStatus(credentialPath).getLen > 0) {
      val cred = new Credentials()
      stream = fs.open(credentialPath)
      cred.readTokenStorageStream(stream)
      info(s"Before Credentials Update ${UGI.getCurrentUser.getCredentials.getAllTokens}")
      info(s"Credentials found ${cred.getAllTokens}")
      UGI.getCurrentUser.addCredentials(cred)
      info(s"After Credentials Update ${UGI.getCurrentUser.getCredentials.getAllTokens}")
    } else {
      info(s"Credential File $credentialFile not updated will try in next cycle")
      closeResource(stream)
    }
  }

  private def genCredentials(credentialsFile: Path, principal: String, keytab: String, nns: Set[Path]): Unit = {
    UGI.setConfiguration(hdfsConf)
    val loggedUser = UGI.loginUserFromKeytabAndReturnUGI(principal, keytab)
    val cred = loggedUser.getCredentials
    loggedUser.doAs(new PrivilegedExceptionAction[Void] {
      override def run(): Void = {
        refreshFsTokens(nns + credentialsFile.getParent, cred)
        null
      }
    })
    UGI.getCurrentUser.addCredentials(cred)
    cred.writeTokenStorageFile(credentialsFile, hdfsConf)
  }

  private def sHook(): Unit = registerHook(newThread(s"$app-Login-Renewer-SHook", runnable(stop())))

}






