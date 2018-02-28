package com.hca.cdm

import java.io.{BufferedWriter, File, FileWriter}

import scala.collection.mutable.ListBuffer
import scala.io.BufferedSource

/**
  * Created by Devaraj Jonnadula on 2/8/2018.
  */
object Temp extends  App {
  val reader =  new BufferedSource(currThread.getContextClassLoader.getResourceAsStream("De-Id-Anonymization-Fields.csv"))
  val out =  new ListBuffer[(String,String)]
  val notReqSeg = Set("ABS","ACC","ADD")
  val r = reader.bufferedReader()
  Stream.continually(r.readLine()).takeWhile(valid(_)).foreach{x =>
    val split = x.split("\\,",-1)
   // if(valid(split,2)) {
      val seg = split(0)

      //println(split(1))
      out += Pair(seg,split(1))
   // }

  }
  val writer = new BufferedWriter(new FileWriter(new File("C:\\Users\\pzi7542\\IdeaProjects\\Cdm-HL7\\cdm-common\\src\\main\\resources\\HL7-Fields.csv")))
 val o = out.foldLeft(EMPTYSTR)((a,b) =>   a+"^"+b._2 )



        writer.write(o)


  writer.write("^message_flag_static^unknown^obsv_value")
  writer.flush()
  writer.close()

}
