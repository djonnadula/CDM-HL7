package com.hca.cdm

import java.io.BufferedOutputStream
import com.hca.cdm.log.Logg
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import scala.util.control.Breaks._

/**
  * Created by Devaraj Jonnadula on 9/20/2016.
  */
package object hadoop extends Logg {

  def overSizedHandle(stage: String, context: SparkContext, destination: String)(data: AnyRef): Unit = {
    try {
      val fs = FileSystem.get(context.hadoopConfiguration)
      val files = fs.listFiles(new Path(destination), false)
      var fileToAppend: Path = null
      breakable {
        while (files.hasNext) {
          if (files.next().isFile) {
            fileToAppend = files.next().getPath
            break
          }
        }
      }
      if (fileToAppend == null) fileToAppend = new Path(destination + FS + stage + currMillis)
      val writer = new BufferedOutputStream(fs.append(fileToAppend))
      data match {
        case x: Array[Byte] => writer write x
        case x: String => writer write (x getBytes UTF8)
        case _ => writer write (data.toString getBytes UTF8)
      }
      writer flush()
      writer close()
    } catch {
      case t: Throwable => error("Unable to Write Oversizes Data to HDFS :: " + data + " At Path :: " + destination.toString)
    }
  }


}
