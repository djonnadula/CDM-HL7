package com.hca.cdm

import java.io._
import java.net.URI
import com.hca.cdm.log.Logg
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import scala.util.{Failure, Success, Try}
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
      var fileToAppend: Path = getFile(fs, destination, config)
      var writer: OutputStream = null
      if (fileToAppend == null) {
        fileToAppend = creteFileAtPath(destination, stage)
      }
      if (config.getBoolean("hdfs.append.support", false)) {
        writer = new BufferedOutputStream(fs append fileToAppend)
      }
      else {
        if (fs exists fileToAppend) fileToAppend = creteFileAtPath(destination, stage)
        writer = new BufferedOutputStream(fs create(fileToAppend, false))
      }
      data match {
        case x: Array[Byte] => writer write x
        case x: String => writer write (x getBytes UTF8)
        case _ => writer write (data.toString getBytes UTF8)
      }
      if (writer != null) {
        writer flush()
        writer close()
      }
    }
    catch {
      case t: Throwable => error(s"Unable to Write Oversize Data to HDFS :: $data At Path :: ${destination.toString}", t)
    }
  }


  private def creteFileAtPath(destination: String, stage: String): Path = new Path(s"$destination$FS$stage-${currNanos}")


  def readObject[T](path: String, config: Configuration, cleanUp: Boolean = true): Option[T] = {
    info(config.toString)
    val fs = FileSystem get config
    val temp = new Path(new URI(path))
    info("Readinf to File " +temp.toString)
    fs.exists(temp) match {
      case true =>
        def fun(): T = {
          var data = null.asInstanceOf[T]
          fs.listStatus(temp).foreach(x => info(x.getPath.toString))
          val status = fs.listStatus(temp).takeWhile(valid(_))
          status.foreach(x => info(x.getPath.toString))
          val f = status.filter(_.isFile)
          f.foreach(x => info(x.getPath.toString))
          val d = f.sortBy(_.getModificationTime)
          d.foreach(x => info(x.getPath.toString))
          d.foreach(x => info(x.toString))
          if (status.nonEmpty) {
            val reader = new BufferedInputStream(fs.open(status.head.getPath))
            data = deSerialize(reader).asInstanceOf[T]
            reader close()
            if (cleanUp) status.foreach(file => fs.delete(file.getPath, false))
            info(data.toString)
          }
          data
        }
        //println("FUNCTION for " + path +"-" +fun)
        tryAndLogErrorMes[T](fun, error(_: String, _: Throwable))
      case _ => None
    }


  }

  def writeObject[T <: Serializable](data: T, name: String, path: String, config: Configuration): Unit = {
    if(valid(data)) {
      info(config.toString)
      info(data.toString)
      info(serialize(data).length.toString)
      val fs = FileSystem get config
      val fileToWrite = getFile(fs, path, config) match {
        case null => creteFileAtPath(path, name)
        case x => x
      }
      info("Writing to File " + fileToWrite.toString)
      val writer = new BufferedOutputStream(fs create(fileToWrite, true))
      Try(writer write serialize(data)) match {
        case Success(x) =>
        case Failure(t) => error(s"Cannot Write Data $data to HDFS at Path :: $path", t)
      }
      writer flush()
      writer close()
    }
  }

  private def getFile(fs: FileSystem, path: String, config: Configuration): Path = {
    val files = Try(fs.listFiles(new Path(new URI(path)), false)) match {
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
    fileToAppend
  }


}
