package com.cdm.hadoop


import java.io.File
import com.cdm._
import com.cdm.log.Logg
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.conf.HiveConf

/**
  * Created by Devaraj Jonnadula on 2/14/2017.
  */
private[cdm] object HadoopConfig extends Logg {

  def loadConfig(configDir: Seq[String]): Configuration = {
    val resources = getConfigFiles(configDir)
    resources foreach (file => info(file.getAbsolutePath))
    val conf = new Configuration()
    if (valid(configDir)) resources.foreach(res => conf.addResource(new File(res.getAbsolutePath).toURI.toURL))
    conf.set("hadoop.security.authentication", "Kerberos")
    conf
  }

  private def getConfigFiles(configDirs: Seq[String]): List[File] = {
    configDirs.foldLeft(List[File]())((a, b) => a ++ getConfigFiles(b))
  }

  private def getConfigFiles(configDir: String): List[File] = {
    val dir = new File(configDir)
    if (dir.exists && dir.isDirectory) {
      return dir.listFiles.filter(_.isFile).filter(_.getName.contains(".xml")).toList
    }
    List[File]()
  }

  def hiveConf(config: Configuration): HiveConf = {
    new HiveConf(config, classOf[HiveConf])

  }
}
