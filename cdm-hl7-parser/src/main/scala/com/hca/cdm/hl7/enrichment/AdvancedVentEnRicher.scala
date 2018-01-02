package com.hca.cdm.hl7.enrichment

import com.hca.cdm.log.Logg
import com.hca.cdm._
import com.hca.cdm.hl7.model._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by Devaraj Jonnadula on 10/5/2017.
  */
class AdvancedVentEnRicher(config: Array[String]) extends Logg with EnrichData {

  private val repeatableFields = readFile(config(0)).getLines().map(x => x -> EMPTYSTR).toMap
  private val obsv_id = "obsv_id"
  private val units = "units"

  override def close(): Unit = {}

  override def apply(layout: mutable.LinkedHashMap[String, String], hl7: String): EnrichedData = {
    val temp = new ListBuffer[mutable.LinkedHashMap[String, String]]
    val transformedData = fieldsAtIndex(layout)
    for (index <- 0 until maxMessages(layout)) {
      val outTemp = layout clone()
      repeatableFields foreach { case (field, _) =>
        if (outTemp isDefinedAt field) {
          outTemp update(field, transformedData(field).getOrElse(index, EMPTYSTR))
          if (field == obsv_id || field == units) {
            def check = transformedData(field).getOrElse(index, EMPTYSTR).toInt

            tryAndThrow(check, error(_: Throwable),
              Some(s"Invalid Data for $field :: ${transformedData(field).getOrElse(index, EMPTYSTR)}"))
          }
        }
      }
      temp += outTemp
    }
    EnrichedData(temp, hl7)
  }

  private def maxMessages(layout: mutable.LinkedHashMap[String, String]): Int = {
    repeatableFields.foldLeft(0)((a, b) =>
      if (layout isDefinedAt b._1) {
        val size = layout(b._1).split(s"\\$caret", -1).length
        if (size >= a) size
        else if (a > 0) a
        else size
      } else a)
  }

  private def fieldsAtIndex(layout: mutable.LinkedHashMap[String, String]): Map[String, Map[Int, String]] = {
    repeatableFields map {
      case (field, _) => field -> layout(field).split(s"\\$caret", -1).view.zipWithIndex.map { case (fld, fldIndex) => fldIndex -> fld }.toMap
    }
  }
}
