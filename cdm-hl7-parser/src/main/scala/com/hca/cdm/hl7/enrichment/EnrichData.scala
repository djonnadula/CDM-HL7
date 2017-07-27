package com.hca.cdm.hl7
package enrichment

import com.hca.cdm._
import com.hca.cdm.hl7.constants.HL7Constants._
import scala.collection.mutable
import scala.language.postfixOps

/**
  * Created by Devaraj Jonnadula on 7/24/2017.
  */
trait EnrichData {

  def close(): Unit

  def apply(layout: mutable.LinkedHashMap[String, String]): Unit

}


private[enrichment] class FacilityCoidHandler(files: Array[String]) extends EnrichData {

  private case class CrossFacilityReference(reqCoid: String, reqFacility: String)

  private val facilityKey = "sending_facility"
  private val coidRefLookUp = "coid_ref_look_up"
  private val patientLocation = "patientLocation"
  private val facilityRef = {
    val temp = new mutable.HashMap[String, mutable.Map[String, String]]
    readFile(files(0)).getLines().takeWhile(valid(_)).map(temp => temp split COMMA).filter(valid(_, 3)).foreach { x =>
      if (temp isDefinedAt x(0)) temp update(x(0), temp(x(0)) += Tuple2(x(1), x(2)))
      else temp += (x(0) -> mutable.Map[String, String](Tuple2(x(1), x(2))))
    }
    temp.toMap
  }
  private val coidCrossRef = readFile(files(1)).getLines().takeWhile(valid(_)).map(temp => temp split COMMA) filter (valid(_, 5)) map {
    x => s"${x(1)}${x(2)}" -> CrossFacilityReference(x(3), x(4))
  } toMap

  override def close(): Unit = {}

  override def apply(layout: mutable.LinkedHashMap[String, String]): Unit = {
    if ((layout isDefinedAt facilityKey) && (facilityRef isDefinedAt layout(facilityKey))) {
      val crossRefFac = (layout isDefinedAt patientLocation) && applyCrossRef(layout(facilityKey), layout(patientLocation), layout)
      if ((layout isDefinedAt patientLocation) && !crossRefFac) {
        if (facilityRef(layout(facilityKey)).get(layout(patientLocation)).isDefined) layout update(coidRefLookUp, facilityRef(layout(facilityKey)).getOrElse(layout(patientLocation), EMPTYSTR))
        else facilityRef(layout(facilityKey)).headOption.foreach(locCoid => layout update(coidRefLookUp, locCoid._2))
      }
    }
  }

  private def applyCrossRef(facility: String, locationCode: String, layout: mutable.LinkedHashMap[String, String]): Boolean = {
    val key = s"$facility$locationCode"
    if (coidCrossRef isDefinedAt key) {
      layout update(facilityKey, coidCrossRef(key).reqFacility)
      layout update(coidRefLookUp, coidCrossRef(key).reqCoid)
      return true
    }
    false
  }
}