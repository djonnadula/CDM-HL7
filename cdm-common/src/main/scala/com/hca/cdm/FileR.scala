package com.hca.cdm

import java.io.{BufferedWriter, File, FileWriter}

import scala.collection.mutable.ListBuffer
import scala.io.BufferedSource

/**
  * Created by Devaraj Jonnadula on 2/8/2018.
  */
object FileR extends  App {
  val reader =  new BufferedSource(currThread.getContextClassLoader.getResourceAsStream("de-id-replay-adhoc-request.txt"))
  val out = new ListBuffer[String]
  val notReqSeg = Set("ABS","ACC","ADD")
val r = reader.bufferedReader()
    val reader1 =  new BufferedSource(currThread.getContextClassLoader.getResourceAsStream("HL7-Fields.csv"))
val p =reader1.bufferedReader().readLine().replaceAll("set_id",EMPTYSTR)
 // println(p)
  Stream.continually(r.readLine()).takeWhile(valid(_)).foreach{x =>
   // println(x)
    val split = x.split("\\,",-1)
    if(split(1).contains(":JSON")) {
      /*if(valid(split,2)) {
      val seg = split(1)

      val buf = new ListBuffer[String]
      split(2).split("\\^", -1).zipWithIndex.
        foreach{x =>
          if ( (x._2 != 1 ) && x._1 != "set_id" && (x._1 != "unknown") && (x._1 != "_unknown")){
          if(seg == "PID" || seg == "PV1") buf +=  (if(!isDate(x._1))  x._1+",DE_ID" else x._1+",DATE")
          else if(seg == "MSH" || seg == "OBX"  ) buf +=  (if(!isDate(x._1))  x._1+",NONE" else x._1+",DATE")
          else buf += (if(!isDate(x._1))  x._1+",Anonymize" else x._1+",DATE")
        }
        }
      out += Pair(seg, buf)
   }*/

      split.last
      val si = split.length - 1
      //println(split(si))
      println(split(split.length - 2).split("\\:", -1)(5).replace("&", "^"))
      split(si) = p + "^" + split(split.length - 2).split("\\:", -1)(5).replace("&", "^")
      out += split.mkString(",")
    }
  }
  private def isDate(x : String) : Boolean ={
    x.contains("date") || x.contains("time")
  }
  val writer = new BufferedWriter(new FileWriter(new File("C:\\Users\\pzi7542\\IdeaProjects\\Cdm-HL7\\cdm-common\\src\\main\\resources\\de-id-replay-adhoc-request-parsed.txt")))
  out.foreach(x => {
    writer.write(x)
    writer.newLine()
  }
  )
  /*out.foreach(x =>
    x._2.foreach { y =>
      writer.write(x._1 + "," + y)
    writer.newLine()
    }
  )*/
  writer.flush()
  writer.close()

}
