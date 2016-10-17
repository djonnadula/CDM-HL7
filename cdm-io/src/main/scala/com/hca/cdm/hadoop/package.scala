package com.hca.cdm

import java.io.{BufferedOutputStream, OutputStream}
import java.net.URI
import com.hca.cdm.log.Logg
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import scala.util.{Success, Try}
import scala.util.control.Breaks._

/**
  * Created by Devaraj Jonnadula on 9/20/2016.
  *
  * Handles Messages whose size greater than {hl7.message.max} and will be routed to Respective HDFS locations.
  */
package object hadoop extends Logg {

  case class OverSizeHandler(stage: String, destination: String) {
    private lazy val config = new Configuration()

    def handle(data: AnyRef): Unit = overSizedHandle(stage, config, destination, data)
  }

  def overSizedHandle(stage: String, config: Configuration, destination: String, data: AnyRef): Unit = {
    try {
      val fs = FileSystem get config
      val files = Try(fs.listFiles(new Path(new URI(destination)), false)) match {
        case Success(x) => x
        case _ => null
      }
      var fileToAppend: Path = null
      if (files != null) breakable {
        Try(while (files.hasNext) {
          if (files.next().isFile) {
            fileToAppend = files.next().getPath
            break
          }
        })
      }
      var writer: OutputStream = null
      if (fileToAppend == null) {
        fileToAppend = getFile(destination, stage)
      }
      if (config.getBoolean("hdfs.append.support", false)) {
        writer = new BufferedOutputStream(fs append fileToAppend)
      }
      else {
        if (fs exists fileToAppend) fileToAppend = getFile(destination, stage)
        writer = new BufferedOutputStream(fs create(fileToAppend, false))
      }
      data match {
        case x: Array[Byte] => writer write x
        case x: String => writer write (x getBytes UTF8)
        case _ => writer write (data.toString getBytes UTF8)
      }
      writer flush()
      writer close()
    }
    catch {
      case t: Throwable => error("Unable to Write Oversize Data to HDFS :: " + data + " At Path :: " + destination.toString, t)
    }
  }


  private def getFile(destination: String, stage: String): Path = {
    new Path(destination + FS + stage + "-" + System.nanoTime())
  }

}
