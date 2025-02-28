package com.cdm.hl7
package enrichment

import com.cdm._
import com.cdm.hl7.constants.HL7Constants._
import com.cdm.log.Logg
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.language.postfixOps

/**
  * Created by Devaraj Jonnadula on 7/24/2017.
  */
case class EnrichedData(enrichedLayout: Any, enrichedHL7: String = EMPTYSTR, rejects: Option[ListBuffer[Throwable]] = None)

trait EnrichData extends Serializable {

  def close(): Unit

  def apply(layout: mutable.LinkedHashMap[String, String], hl7: String): EnrichedData

}

trait EnrichDataFromOffHeap extends EnrichData with Serializable {

  protected var enrichDataPartFun: (String, String, String, Set[String]) => mutable.Map[String, Array[Byte]] = _
  protected var partSelectorFun: (String, String, Set[String], Int, String, String) => Map[Int, mutable.Map[String, Array[Byte]]] = _
  protected var partWriterFun: (Map[String, (String, String, mutable.Map[String, String], Boolean) => Unit]) = _

  def apply(enrichData: (String, String, String, Set[String]) => mutable.Map[String, Array[Byte]], layout: mutable.LinkedHashMap[String, String], hl7: String): EnrichedData


  def init(handlers: ((String, String, String, Set[String]) => mutable.Map[String, Array[Byte]],
    (String, String, Set[String], Int, String, String) => Map[Int, mutable.Map[String, Array[Byte]]], (Map[String, (String, String, mutable.Map[String, String], Boolean) => Unit]))): Unit = {
    enrichDataPartFun = handlers._1
    partSelectorFun = handlers._2
    partWriterFun = handlers._3
  }

  def repos: Set[String]

}

case class NoEnRicher() extends EnrichDataFromOffHeap {
  override def close(): Unit = {}

  override def apply(layout: mutable.LinkedHashMap[String, String], hl7: String): EnrichedData = EnrichedData(layout, hl7)

  override def apply(enrichData: (String, String, String, Set[String]) => mutable.Map[String, Array[Byte]],
                     layout: mutable.LinkedHashMap[String, String], hl7: String): EnrichedData = EnrichedData(layout, hl7)

  def repos: Set[String] = Set.empty
}

private[enrichment] class FacilityCoidHandler(files: Array[String]) extends EnrichData {

  private case class CrossFacilityReference(reqCoid: String, reqFacility: String, regNetworkMnemonic: String)

  private case class FacilityReference(reqCoid: String, regNetworkMnemonic: String, facilityDesc: String)


  private lazy val facilityKey = "sending_facility"
  private lazy val coidRefLookUp = "coid_ref_look_up"
  private lazy val patientLocation = "patientLocation"
  private lazy val networkID = "meditech_network_id"
  private lazy val facility_desc = "facility_desc"
  private val facilityRefData = readFile(files(0)).getLines().toList
  private val coidCrossRefData = readFile(files(1)).getLines().toList
  private lazy val facilityRef = {
    val temp = new mutable.HashMap[String, mutable.Map[String, FacilityReference]]
    facilityRefData.takeWhile(valid(_)).map(temp => temp split COMMA).filter(valid(_, 4)).foreach { x =>
      if (temp isDefinedAt trimStr(x(0))) temp update(trimStr(x(0)), temp(trimStr(x(0))) += Pair(trimStr(x(1)), FacilityReference(trimStr(x(2)), trimStr(x(3)), trimStr(x(4)))))
      else temp += (trimStr(x(0)) -> mutable.Map[String, FacilityReference](Pair(trimStr(x(1)), FacilityReference(trimStr(x(2)), trimStr(x(3)), trimStr(x(4))))))
    }
    temp.toMap
  }
  private lazy val coidCrossRef = coidCrossRefData.takeWhile(valid(_)).map(temp => temp split COMMA) filter (valid(_, 5)) map {
    x => s"${trimStr(x(1))}${trimStr(x(2))}" -> CrossFacilityReference(trimStr(x(3)), trimStr(x(4)), trimStr(x(5)))
  } toMap

  override def close(): Unit = {}

  override def apply(layout: mutable.LinkedHashMap[String, String], hl7: String): EnrichedData = {
    if ((layout isDefinedAt facilityKey) && (facilityRef isDefinedAt layout(facilityKey))) {
      if ((layout isDefinedAt patientLocation) && !isCrossRefFac(layout)) {
        if (facilityRef(layout(facilityKey)).get(layout(patientLocation)).isDefined) {
          facilityRef(layout(facilityKey)).get(layout(patientLocation)).foreach { facRef =>
            layout update(coidRefLookUp, facRef.reqCoid)
            layout update(networkID, facRef.regNetworkMnemonic)
            layout update(facility_desc, facRef.facilityDesc)
          }
        }
        else facilityRef(layout(facilityKey)).headOption.foreach { case (_, facRef) =>
          layout update(coidRefLookUp, facRef.reqCoid)
          layout update(networkID, facRef.regNetworkMnemonic)
          layout update(facility_desc, facRef.facilityDesc)
        }
      }
    }
    EnrichedData(layout, hl7)
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

private[enrichment] class PatientEnRicher(config: Array[String]) extends EnrichDataFromOffHeap with Logg {
  self =>

  private lazy val fieldsToSkip = "patient_address|other_designation"
  private val enrichSourceToTargetMapping = com.hca.cdm.hl7.model.loadFileAsList(config(1))
  private val enrichAttributes = enrichSourceToTargetMapping.map(_._2).toSet
  private val cfg: OffHeapConfig = {
    val dest = config(0) split "\\&"
    OffHeapConfig(dest(0), dest(2), dest(3).split("\\;", -1).toSet)
  }

  override def close(): Unit = {}


  override def apply(enrichData: (String, String, String, Set[String]) => mutable.Map[String, Array[Byte]], layout: mutable.LinkedHashMap[String, String], hl7: String): EnrichedData = {
    if (fetchRequired(layout)) {
      val res = enrichData(cfg.repo, cfg.identifier, cfg.fetchKey(layout), enrichAttributes).map { case (k, v) => k -> new String(v, UTF8) }
      enrichSourceToTargetMapping.foreach {
        case (enrichField, reqEnrichField) =>
          if ((layout isDefinedAt enrichField) && layout(enrichField) == EMPTYSTR && (res isDefinedAt reqEnrichField) && res(reqEnrichField) != EMPTYSTR) {
            layout update(enrichField, res(reqEnrichField))
          }
      }
    }
    EnrichedData(layout, hl7)

  }

  def fetchRequired(layout: mutable.LinkedHashMap[String, String]): Boolean = {
    var fetch = false
    enrichSourceToTargetMapping.foreach {
      case (enrichField, _) =>
        if (enrichField != fieldsToSkip) fetch = layout.getOrElse(enrichField, EMPTYSTR) == EMPTYSTR
        if (fetch) return true
    }
    fetch
  }


  override def apply(layout: mutable.LinkedHashMap[String, String], hl7: String): EnrichedData = {
    apply(self.enrichDataPartFun, layout, hl7)
  }

  def repos: Set[String] = Set(cfg.repo)
}

private[cdm] case class OffHeapConfig(repo: String, identifier: String, fetchKeyAttributes: Set[String]) extends Logg {

  def fetchKey(layout: mutable.LinkedHashMap[String, String]): String = fetchKeyAttributes.foldLeft(EMPTYSTR)((a, b) => a + layout.getOrElse(b, EMPTYSTR))


}
