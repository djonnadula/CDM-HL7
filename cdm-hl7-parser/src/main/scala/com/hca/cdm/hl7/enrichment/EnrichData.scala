package com.hca.cdm.hl7
package enrichment

import com.hca.cdm._
import com.hca.cdm.hl7.constants.HL7Constants._
import scala.collection.mutable
import scala.language.postfixOps

/**
  * Created by Devaraj Jonnadula on 7/24/2017.
  */
trait EnrichData extends Serializable {

  def close(): Unit

  def apply(layout: mutable.LinkedHashMap[String, String]): Unit

}

class NoEnricher() extends EnrichData {
  override def close(): Unit = {}

  override def apply(layout: mutable.LinkedHashMap[String, String]): Unit = {}
}

private[enrichment] class FacilityCoidHandler(files: Array[String]) extends EnrichData {

  private case class CrossFacilityReference(reqCoid: String, reqFacility: String, regNetworkMnemonic: String)

  private case class FacilityReference(reqCoid: String, regNetworkMnemonic: String)


  private lazy val facilityKey = "sending_facility"
  private lazy val coidRefLookUp = "coid_ref_look_up"
  private lazy val patientLocation = "patientLocation"
  private lazy val networkID = "meditech_network_ID"
  private val facilityRefData = readFile(files(0)).getLines().toList
  private val coidCrossRefData = readFile(files(1)).getLines().toList
  private lazy val facilityRef = {
    val temp = new mutable.HashMap[String, mutable.Map[String, FacilityReference]]
    facilityRefData.takeWhile(valid(_)).map(temp => temp split COMMA).filter(valid(_, 4)).foreach { x =>
      if (temp isDefinedAt x(0)) temp update(x(0), temp(x(0)) += Tuple2(x(1), FacilityReference(x(2), x(3))))
      else temp += (x(0) -> mutable.Map[String, FacilityReference](Tuple2(x(1), FacilityReference(x(2), x(3)))))
    }
    temp.toMap
  }
  private lazy val coidCrossRef = coidCrossRefData.takeWhile(valid(_)).map(temp => temp split COMMA) filter (valid(_, 5)) map {
    x => s"${x(1)}${x(2)}" -> CrossFacilityReference(x(3), x(4), x(5))
  } toMap

  override def close(): Unit = {}

  override def apply(layout: mutable.LinkedHashMap[String, String]): Unit = {
    if ((layout isDefinedAt facilityKey) && (facilityRef isDefinedAt layout(facilityKey))) {
      if ((layout isDefinedAt patientLocation) && !isCrossRefFac(layout)) {
        if (facilityRef(layout(facilityKey)).get(layout(patientLocation)).isDefined) {
          facilityRef(layout(facilityKey)).get(layout(patientLocation)).foreach { facRef =>
            layout update(coidRefLookUp, facRef.reqCoid)
            layout update(networkID, facRef.regNetworkMnemonic)
          }
        }
        else facilityRef(layout(facilityKey)).headOption.foreach { case (loc, facRef) =>
          layout update(coidRefLookUp, facRef.reqCoid)
          layout update(networkID, facRef.regNetworkMnemonic)
        }
      }
    }
  }

  private def isCrossRefFac(layout: mutable.LinkedHashMap[String, String]): Boolean = {
    (layout isDefinedAt patientLocation) && applyCrossRef(layout(facilityKey), layout(patientLocation), layout)
  }

  private def applyCrossRef(facility: String, locationCode: String, layout: mutable.LinkedHashMap[String, String]): Boolean = {
    val key = s"$facility$locationCode"
    if (coidCrossRef isDefinedAt key) {
      layout update(facilityKey, coidCrossRef(key).reqFacility)
      layout update(coidRefLookUp, coidCrossRef(key).reqCoid)
      layout update(networkID, coidCrossRef(key).regNetworkMnemonic)
      return true
    }
    false
  }
}