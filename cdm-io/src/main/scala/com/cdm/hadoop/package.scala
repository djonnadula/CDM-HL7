package com.cdm

import java.io._
import java.net.URI

import org.apache.hadoop.io._
import org.apache.spark.deploy.SparkHadoopUtil.{get => hdpUtil}
import com.cdm.log.Logg
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.{FileInputFormat, SequenceFileInputFormat}
import parquet.hadoop.mapred.DeprecatedParquetInputFormat

import scala.util.{Failure, Success, Try}
import scala.util.control.Breaks._

/**
  * Created by Devaraj Jonnadula on 9/20/2016.
  *
  * Handles Messages whose size greater than {hl7.message.max} and will be routed to Respective HDFS locations.
  */
package object hadoop extends Logg {

  def hadoopConf : Configuration = {
    val conf =  HBaseConfiguration.create(hdpUtil.conf)
    conf.addResource("hbase-site.xml")
    conf.set("hadoop.security.authentication", "Kerberos")
    conf
  }

  case class OverSizeHandler(stage: String, destination: String) {
    private lazy val config = hadoopConf

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


  private def creteFileAtPath(destination: String, stage: String): Path = new Path(s"$destination$FS$stage-$currNanos")


  def readObject[T](path: String, config: Configuration, cleanUp: Boolean = true): Option[T] = {
    val fs = FileSystem get config
    val temp = new Path(new URI(path))
    info(s"Reading Data from Path $temp")
    if (fs.exists(temp)) {
      def fun(): T = {
        var data = null.asInstanceOf[T]
        val status = fs.listStatus(temp).filter(valid(_)).takeWhile(_.getLen > 0L).toList.sortBy(_.getModificationTime)
        if (status.nonEmpty) {
          val reader = new BufferedInputStream(fs.open(status.head.getPath))
          data = deSerialize[T](reader)
          reader close()
          if (cleanUp) fs.listStatus(temp).filter(_.isFile).foreach(file => fs.delete(file.getPath, false))
        }
        data
      }

      tryAndLogErrorMes[T](fun, warn(_: String, _: Throwable))
    } else {
      None
    }
  }

  def writeObject[T <: Serializable](data: T, name: String, path: String, config: Configuration): Unit = {
    if (valid(data)) {
      val fs = FileSystem get config
      val fileToWrite = getFile(fs, path, config) match {
        case null => creteFileAtPath(path, name)
        case x => x
      }
      info(s"Writing to File $fileToWrite")
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

  private def deleteFiles(fs: FileSystem, path: Path): Unit = {
    if (fs isDirectory path) fs delete(path, true)
  }

}

object Format extends  Enumeration{
  type FileFormat = Value
  val parquet = Value("parquet")
  val sequence = Value("sequence")
  val default = sequence

/*  def getFormat(fileFormat: FileFormat) : (Class[Writable],Class[Writable],Class[FileInputFormat[Writable,Writable]]) ={
    fileFormat match {
     case  Format.sequence =>
       (classOf[LongWritable],classOf[Text],(SequenceFileInputFormat[LongWritable,Text]).getClass)
     case Format.parquet =>
     case _ => throw  new UnsupportedOperationException(s"Format $fileFormat not yet impl")
   }
  }*/
}

class ParqF[K,V] extends DeprecatedParquetInputFormat[V]

