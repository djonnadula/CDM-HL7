package com.hca.cdm.utils

import scala.collection.mutable

/**
  * Created by Devaraj Jonnadula (de08698) on 9/28/2016.
  */
object Filters {

  object Conditions extends Enumeration {
    type Condition = Value
    val EQUAL = Value("EQUAL")
    val NOTEQUAL = Value("NOTEQUAL")
    val GT = Value("GT")
    val LT = Value("LT")
    val GTE = Value("GTE")
    val LTE = Value("LTE")
    val CONTAINS = Value("CONTAINS")
    val STARTSWITH = Value("STARTSWITH")
    val COMPARE_IGNORE_CASE_EQUAL = Value("COMPARE_IGNORE_CASE_EQUAL")
    val COMPARE = Value("COMPARE")
  }

  object Expressions extends Enumeration {
    type Expression = Value
    val AND = Value("AND")
    val OR = Value("OR")
    val NONE = Value("NONE")
  }

  object MultiValues extends Enumeration {
    type MultiValueRange = Value
    val IN = Value("IN")
    val BETWEEN = Value("BETWEEN")
    val NA = Value("NA")
  }

  import com.hca.cdm.utils.Filters.Conditions._
  import com.hca.cdm.utils.Filters.Expressions._
  import com.hca.cdm.utils.Filters.MultiValues._


  case class FILTER(segment: String, path: (String, String), filter: (Condition, Expression), multiValues: Option[MultiValueRange]) {
    lazy val matchPath: Array[String] = synchronized {
      if (path._1 contains "|") {
        path._1 split("\\|", -1)
      } else {
        val temp = new Array[String](1)
        temp(0) = path._1
        temp
      }
    }
    lazy val multipleValues: AnyRef = synchronized {
      multiValues.get match {
        case IN =>
          path._2.split(":").map(x => x -> "").toMap
        case BETWEEN =>
          path._2.split(":")
      }
    }

  }


}


