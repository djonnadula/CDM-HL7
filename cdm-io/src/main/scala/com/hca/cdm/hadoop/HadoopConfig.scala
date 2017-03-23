package com.hca.cdm.hadoop


import java.io.File
import com.hca.cdm._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.conf.HiveConf

/**
  * Created by Devaraj Jonnadula on 2/14/2017.
  */
private[cdm] object HadoopConfig {

  def loadConfig(configDir: String): Configuration = {
    getConfigFiles(configDir).foreach(file => info(file.getAbsolutePath))
    val conf = new Configuration()
    conf.set("hadoop.security.authentication", "Kerberos")
    if (valid(configDir)) getConfigFiles(configDir).foreach(res => conf.addResource(new File(res.getAbsolutePath).toURI.toURL))
    conf
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
