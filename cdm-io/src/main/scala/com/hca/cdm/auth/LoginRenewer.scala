package com.hca.cdm.auth

import java.io._
import java.util.concurrent.TimeUnit._
import com.hca.cdm.{lookUpProp, _}
import com.hca.cdm.log.Logg
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.security.{UserGroupInformation => UGI, _}
import java.lang.System.{getenv => fromEnv}
import collection.JavaConverters._
import scala.language.postfixOps
import java.security.PrivilegedExceptionAction
import com.hca.cdm.exception.CdmException
import com.hca.cdm.hadoop.HadoopConfig
import org.apache.hadoop.hbase.security.token.AuthenticationTokenIdentifier
import org.apache.hadoop.mapred.Master
import org.apache.spark.SparkConf
import org.apache.hadoop.hbase.security.token.TokenUtil._
import org.apache.hadoop.security.token.{Token, TokenIdentifier}
import scala.collection.mutable.ListBuffer

/**
  * Created by Devaraj Jonnadula on 2/15/2017.
  */
private[cdm] object LoginRenewer extends Logg {
  self =>
  private val credentials = "Credentials"
  private lazy val loginRenewer = newDaemonScheduler("Token-Renewer")
  private lazy val tryRenewal = tryAndReturnDefaultValue0(lookUpProp("hl7.hdfs.token.renewal").toBoolean, true)
  private lazy val hBaseRenewal = tryAndReturnDefaultValue0(lookUpProp("hl7.hbase.token.renewal").toBoolean, false)
  private var hdfsConf: Configuration = _
  private var sparkConf: SparkConf = _
  private var fs: FileSystem = _
  private var scheduled = false
  private val lock = new Object


  def scheduleRenewal(master: Boolean = false, namesNodes: String = EMPTYSTR, conf: Option[Configuration] = None): Boolean = lock.synchronized {
    if (!scheduled && isSecured) {
      hdfsConf = conf.getOrElse(hadoop.hadoopConf)
      fs = FileSystem.get(hdfsConf)
      val appHomeDir = fs.getHomeDirectory
      hdfsConf = byPassConfig(appHomeDir.toUri.getScheme, hdfsConf)
      val tempCrd = credentialFilePath(s"$appHomeDir$FS${fromEnv("SPARK_YARN_STAGING_DIR")}")
      sparkConf = new SparkConf
      info(s"Credentials File set to $tempCrd and Staging Dir ${fromEnv("SPARK_YARN_STAGING_DIR")}")
      if (!master) sparkConf.set("spark.yarn.access.namenodes", if (namesNodes == EMPTYSTR) lookUpProp("secure.name.nodes") else namesNodes)
      if (master) scheduleGenCredentials(2, tempCrd, sparkConf.get("spark.yarn.principal"), sparkConf.get("spark.yarn.keytab"), haNameNodes(sparkConf))
      else scheduleLoginFromCredentials(2.1.toLong, tempCrd)
    }
    scheduled
  }

  def isSecured: Boolean = UGI.isSecurityEnabled && tryRenewal

  private def scheduleLoginFromCredentials(startFrom: Long = 6, credentialsFile: Path): Unit = lock.synchronized {
    if (!scheduled) {
      loginRenewer scheduleAtFixedRate(runnable(tryAndLogErrorMes(accessCredentials(credentialsFile), error(_: Throwable))),
        0, MILLISECONDS.convert(startFrom, HOURS), MILLISECONDS)
      sHook()
      scheduled = true
    }
  }

  def loginFromKeyTab(keyTab: String, principal: String, config: Option[Configuration]): Boolean = {
    info(s"Logging $principal with KeyTab $keyTab")
    config.foreach(cfg => UGI.setConfiguration(cfg))
    UGI.loginUserFromKeytab(principal, keyTab)
    val currUser = UGI.getLoginUser
    if (valid(currUser) && currUser.hasKerberosCredentials && currUser.isFromKeytab) {
      info(s"Login successful for ${currUser.getUserName} and credentials ${currUser.getCredentials}")
      setCredentials(currUser)
      return true
    } else error(s"Login failed for $principal")
    false
  }

  private def setCredentials(user: UGI): Unit = {
    UGI.getCurrentUser.addCredentials(user.getCredentials)
  }

  private def scheduleGenCredentials(startFrom: Long = 1, credentialsFile: Path, principal: String, keyTab: String, nns: Set[Path]): Unit = {
    loginRenewer scheduleAtFixedRate(runnable(tryAndLogErrorMes(genCredentials(Left(credentialsFile), principal, keyTab, nns), error(_: Throwable)))
      , 0, MILLISECONDS.convert(startFrom, HOURS), MILLISECONDS)
    sHook()
  }

  private def credentialFilePath(stagingDir: String): Path = {
    val crPath = new Path(s"$stagingDir$FS$credentials")
    performAction(asFunc(fs.createNewFile(crPath)))
    crPath
  }

  def credentialFile(stagingDir: String): String = s"$stagingDir$FS$credentials"

  private def haNameNodes(conf: SparkConf): Set[Path] = {
    val nds = conf.get("spark.yarn.access.namenodes", EMPTYSTR).split(",")
      .map(_.trim).filter(!_.isEmpty).map(new Path(_)).toSet
    info(s"haNameNodes $nds")
    nds
  }

  def stop(): Unit = {
    loginRenewer shutdown()
    loginRenewer awaitTermination(1, MINUTES)
    info(s"Login-Renewer Shutdown Completed")
  }

  @throws[CdmException]
  def performAction[T](fun: () => T, user: Option[UGI] = None): T = {
    tryAndLogErrorMes(user.getOrElse(UGI.getLoginUser).checkTGTAndReloginFromKeytab(), warn(_: Throwable))
    tryAndThrow({
      user.getOrElse(UGI.getCurrentUser).doAs(new PrivilegedExceptionAction[T] {
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

  private def refreshFsTokens(nameNodes: Set[Path]): ListBuffer[Token[TokenIdentifier]] = {
    val tokens = new ListBuffer[Token[TokenIdentifier]]
    val renewer = Master.getMasterPrincipal(hdfsConf)
    info(s"Renewer for Credentials $renewer for $nameNodes")
    nameNodes foreach { node =>
      val dfs = node.getFileSystem(hdfsConf)
      try {
        val token = dfs.getDelegationToken(renewer)
        info(s"Refreshed Tokens for node $node $token")
        tokens += token.asInstanceOf[Token[TokenIdentifier]]
      }
      catch {
        case t: Throwable => error(s"cannot Refresh Tokens for $node & FileSystem $dfs", t)
      }
    }
    tokens
  }

  private def accessCredentials(credentialPath: Path): Unit = {
    performAction(asFunc({
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
        info(s"Credential File $credentialPath not updated will try in next cycle")
      }
      closeResource(stream)
    }))
  }

  private def genCredentials(credentialsFile: Either[Path, File], principal: String, keyTab: String, nns: Set[Path]): Unit = {
    UGI.loginUserFromKeytab(principal, keyTab)
    val loggedUser = UGI.getLoginUser
    val cred = loggedUser.getCredentials
    val tokens = performAction(asFunc({
      info("generating Token for user=" + loggedUser.getUserName + ", authMethod=" + loggedUser.getAuthenticationMethod)
      credentialsFile match {
        case Left(path) => refreshFsTokens(nns + path.getParent)
        case Right(_) => refreshFsTokens(nns)
      }
    }), Some(loggedUser))
    renewHBaseToken(principal, keyTab, Some(loggedUser)).foreach { tkn => tokens += tkn.asInstanceOf[Token[TokenIdentifier]] }
    tokens.foreach(tkn => cred.addToken(tkn.getService, tkn))
    loggedUser.addCredentials(cred)
    setCredentials(loggedUser)
    if (credentialsFile.isLeft) cred.writeTokenStorageFile(credentialsFile.left.get, hdfsConf)
    else if (credentialsFile.isRight) {
      val outStream = new DataOutputStream(new FileOutputStream(credentialsFile.right.get))
      cred writeTokenStorageToStream outStream
      closeResource(outStream)
    }
  }

  private def renewHBaseToken(principal: String, keyTab: String, user: Option[UGI] = None): Option[Token[AuthenticationTokenIdentifier]] = {
    performAction(asFunc({
      // noinspection ScalaDeprecation
      if (tryAndReturnDefaultValue0(sparkConf.getBoolean("spark.yarn.security.tokens.hbase.enabled", defaultValue = hBaseRenewal), hBaseRenewal)) {
        val hBaseToken = obtainToken(hdfsConf)
        if (valid(hBaseToken)) {
          info(s"Refreshed HBase Token $hBaseToken")
          Some(hBaseToken)
        } else None
      } else None
    }), Some(user.getOrElse(UGI.loginUserFromKeytabAndReturnUGI(principal, keyTab))))
  }

  def createTokensFromDriver(app: String, principal: String, keyTab: String, configDir: Seq[String]): Option[File] = {
    kInit(keyTab, principal)
    tryAndReturnDefaultValue0({
      new File(s"/tmp/$app").mkdirs()
      val tempCredentialFile = new File(s"/tmp/$app/$credentials")
      hdfsConf = HadoopConfig loadConfig configDir
      UGI setConfiguration hdfsConf
      genCredentials(Right(tempCredentialFile), principal, keyTab, Set(new Path(tryAndReturnDefaultValue0(lookUpProp("secure.name.nodes"), "hdfs://nameservice1"))))
      Some(tempCredentialFile)
    }, None)
  }

  def kInit(keyTab: String, principal: String): Unit = {
    val commands = new ListBuffer[String]
    commands += "/usr/bin/kinit"
    commands += "-k"
    commands += "-t"
    commands += keyTab
    commands += principal
    if (executeScript(commands.asJava)) info(s"kInit completed for $commands")
    else info(s"kInit failed for $commands")
  }

  private def sHook(): Unit = registerHook(newThread("Login-Renewer-SHook", runnable(stop())))

}






