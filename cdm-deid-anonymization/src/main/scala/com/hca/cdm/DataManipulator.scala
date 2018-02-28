package com.hca.cdm

import java.text.SimpleDateFormat
import com.hca.cdm.hl7.enrichment.{EnrichDataFromOffHeap, EnrichedData, OffHeapConfig}
import com.hca.cdm.log.Logg
import com.hca.cdm.hl7.constants.HL7Constants._
import scala.collection.mutable
import com.hca.cdm.hl7.model._
import java.util.{Date, UUID => uidGenarator}
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.atomic.AtomicBoolean
import com.hca.cdm.utils.DateConstants._
import org.apache.commons.lang3.StringUtils
import org.joda.time.DateTime
import scala.collection.concurrent.TrieMap
import scala.util.{Failure, Success, Try}


/**
  * Created by Devaraj Jonnadula on 1/22/2018.
  */


private[cdm] object DataManipulations extends Enumeration {
  self =>
  type OP = Value
  val DE_ID: Value = Value("DE_ID")
  val ANONYMIZE: Value = Value("Anonymize")
  val NONE: Value = Value("NONE")
  val DEFAULT: Value = ANONYMIZE
  val DATE: Value = Value("DATE")

}

private[cdm] class DataManipulator(config: Array[String]) extends EnrichDataFromOffHeap with Logg {
  self =>

  import DataManipulations._
  import Patterns._

  private val random = ThreadLocalRandom.current()
  private val maxRandomSelects = 100000
  private val randomCfg = Config(config(0))
  private val deIdCfg = Config(config(1))
  private val orgCfg = Config(config(2))
  private val dataOperators = loadFileAsList(config(3), COMMA, 1).map { case (field, op) => field -> tryAndReturnDefaultValue0(withName(op), DEFAULT) }.toMap
  private val deIdFields = dataOperators.filter(_._2 == DE_ID).keySet
  private val deIdFieldsRdm = deIdFields.map(_.replaceAll("\\|", "_"))
  private val fetchReq = new AtomicBoolean(deIdFields.nonEmpty)
  private val facilityRef = loadFileAsList(config(4)).map(_._1.trim).filter(_.startsWith("COC")).toArray
  private val facilityNames = loadFileAsList(config(4), COMMA, 3).map(_._2.trim).map(x => x -> x.split(" ", -1).map(_.trim).filter(_.length > 1))
  private val hl7_mappings = loadFileAsList(config(5)).map { case (k, v) => v -> k }.toMap
  private val obsv_value = "obsv_value"
  private val ssn = "ssn_num_patient"
  private val identifiers = Set("medical_record_num", "medical_record_urn", "patient_account_num").map(x => x -> x).toMap
  private lazy val fac = getFac
  private val lock = new Object
  private val dummyFac = "NO-FAC"
  private lazy val randomIdentifiers = lock.synchronized {
    partSelectorFun(randomCfg.repo, randomCfg.identifier, identifiers.keySet, 15000, fac._1, fac._2).map { case (ind, store) => ind -> store.map { case (k, v) => hl7_mappings.getOrElse(k, k) -> new String(v, UTF8) } }
  }
  private var randomCache = new TrieMap[Int, mutable.Map[String, String]]()


  def apply(enrichData: (String, String, String, Set[String]) => mutable.Map[String, Array[Byte]], layout: mutable.LinkedHashMap[String, String], hl7: String): EnrichedData = {
    if (fetchReq.get()) {
      val fetchedResults = partSelectorFun(randomCfg.repo, randomCfg.identifier, deIdFieldsRdm, maxRandomSelects / 30, fac._1, fac._2
      ).map { case (ind, store) => ind -> store.map { case (k, v) => hl7_mappings.getOrElse(k, k) -> new String(v, UTF8) } }
      val mx = randomCache.size
      fetchedResults.foreach { case (ind, store) => randomCache += mx + ind -> store }
      fetchReq.set(enoughCache)
    }
    val org = layout clone()
    val deIdentified = enrichData(deIdCfg.repo, deIdCfg.identifier, deIdCfg.fetchKey(org), deIdFields).map { case (k, v) => k -> new String(v, UTF8) }
    var hl7Mod = hl7
    var obs_note = layout.getOrElse(obsv_value, EMPTYSTR)
    val facility = layout.getOrElse("sending_facility", EMPTYSTR)
    val messageControlId = layout.getOrElse("message_control_id", EMPTYSTR)
    val sourceSystem = tryAndReturnDefaultValue0(messageControlId.substring(0, messageControlId.indexOf(underScore)), EMPTYSTR)
    hl7Mod = handleId(messageControlId, hl7Mod, layout,deIdentified).replaceAll(facility,dummyFac)
    obs_note = obs_note.replaceAll(facility,dummyFac)
    dataOperators.foreach {
      case (enrichField, op) =>
        if ((layout isDefinedAt enrichField) && layout(enrichField) != EMPTYSTR) {
          op match {
            case DE_ID =>
              if ((deIdentified isDefinedAt enrichField) && deIdentified(enrichField) != EMPTYSTR) {
                handleCases(enrichField, org(enrichField), deIdentified(enrichField), layout)
                hl7Mod = handleText(hl7Mod, enrichField, org(enrichField), layout(enrichField))(facility, sourceSystem)
                obs_note = handleText(obs_note, enrichField, org(enrichField), layout(enrichField))(facility, sourceSystem)
              } else if ((deIdentified isDefinedAt enrichField) && deIdentified(enrichField) == EMPTYSTR) {
                val randomSelect = getRandomFromStore(enrichField, layout(enrichField))
                handleCases(enrichField, org(enrichField), randomSelect, layout)
                hl7Mod = handleText(hl7Mod, enrichField, org(enrichField), layout(enrichField))(facility, sourceSystem)
                obs_note = handleText(obs_note, enrichField, org(enrichField), layout(enrichField))(facility, sourceSystem)
              } else if (!(deIdentified isDefinedAt enrichField)) {
                val randomSelect = getRandomFromStore(enrichField, layout(enrichField))
                handleCases(enrichField, org(enrichField), randomSelect, layout)
                hl7Mod = handleText(hl7Mod, enrichField, org(enrichField), layout(enrichField))(facility, sourceSystem)
                obs_note = handleText(obs_note, enrichField, org(enrichField), layout(enrichField))(facility, sourceSystem)
              } else {
                val randomSelect = getRandomFromStore(enrichField, layout(enrichField))
                handleCases(enrichField, org(enrichField), randomSelect, layout)
                hl7Mod = handleText(hl7Mod, enrichField, org(enrichField), layout(enrichField))(facility, sourceSystem)
                obs_note = handleText(obs_note, enrichField, org(enrichField), layout(enrichField))(facility, sourceSystem)
              }
            case ANONYMIZE | DEFAULT =>
              layout update(enrichField, EMPTYSTR)
              hl7Mod = handleText(hl7Mod, enrichField, org(enrichField), layout(enrichField))(facility, sourceSystem)
              obs_note = handleText(obs_note, enrichField, org(enrichField), layout(enrichField))(facility, sourceSystem)
            case DATE =>
              val modDate = handleDates(layout(enrichField))
              val formattedDt = getAlternateDateFrmt(modDate._2)
              hl7Mod = hl7Mod.replaceAll(formattedDt, EMPTYSTR)
              obs_note = obs_note.replaceAll(formattedDt, EMPTYSTR)
              layout update(enrichField, modDate._1)
              hl7Mod = handleText(hl7Mod, enrichField, org(enrichField), layout(enrichField))(facility, sourceSystem)
              obs_note = handleText(obs_note, enrichField, org(enrichField), layout(enrichField))(facility, sourceSystem)
            case NONE =>
          }
        }
    }
    hl7Mod = alterDOB(hl7Mod)
    obs_note = alterDOB(obs_note)
    val facilityMod = handleFacilityNames(hl7Mod, obs_note, facility, sourceSystem)
    hl7Mod = facilityMod._1
    obs_note = facilityMod._2
    if (layout isDefinedAt obsv_value) layout update(obsv_value, obs_note)
    self.partWriterFun(orgCfg.repo).apply(orgCfg.identifier, orgCfg.fetchKey(org), org, true)
    self.partWriterFun(deIdCfg.repo).apply(deIdCfg.identifier, deIdCfg.fetchKey(org), layout, true)
    EnrichedData(layout, hl7Mod)
  }

  private def handleId(id: String, hl7: String, layout: mutable.LinkedHashMap[String, String], deIdentified: mutable.Map[String,String] ): String = {
    val random = uidGenarator.randomUUID().toString
    layout update("message_control_id", deIdentified.getOrElse("message_control_id",random))
    tryAndReturnDefaultValue0(hl7 replaceAll(id, deIdentified.getOrElse("message_control_id",random)), hl7)
  }

  private def handleFacilityNames(hl7: String, notes: String, facility: String, sourceSystem: String): (String, String) = {
    var tempHl7 = hl7
    var tempNotes = notes
    facilityNames.foreach {
      fac =>
        tempHl7 = tryAndReturnDefaultValue0(tempHl7.replaceAll(fac._1, EMPTYSTR), tempHl7)
        tempNotes = tryAndReturnDefaultValue0(tempNotes.replaceAll(fac._1, EMPTYSTR), tempNotes)
        fac._2.foreach { splits =>
          if (splits != EMPTYSTR && splits.length > 2 && splits != "MSH" && splits != "Final" && splits != "LAB" && !splits.equalsIgnoreCase("lab") && splits != "Lab" && splits != "PAT" && splits != "PATH" && splits != "Path" && splits != "SOUT" && splits != "FOUT"
            && splits != facility && splits != sourceSystem && splits!= "The") {
            tempHl7 = tryAndReturnDefaultValue0(tempHl7.replaceAll(splits, EMPTYSTR), tempHl7)
            tempNotes = tryAndReturnDefaultValue0(tempNotes.replaceAll(splits, EMPTYSTR), tempNotes)
          }
        }
    }
    (tempHl7, tempNotes)
  }

  private def handleCases(field: String, org: String, data: String, layout: mutable.LinkedHashMap[String, String]): Unit = {
    if (field == ssn) layout update(field, handleSsn(data))
    if (identifiers.isDefinedAt(field)) {
      if (org != EMPTYSTR) layout update(field, handleIdentifiers(org))
      else layout update(field, handleIdentifiers(data))
    }
    else layout update(field, data)
  }

  private def handleSsn(num: String): String = {
    num.toCharArray.foldLeft(EMPTYSTR)((a, b) => if (b == '-') a + b else {
      tryAndReturnDefaultValue0(Integer.parseInt(b + EMPTYSTR) + random.nextInt(9), random.nextInt(9)) match {
        case x => if (x < 10) a + x else a + b
      }
    })
  }

  private def handleIdentifiers(num: String): String = {
    num.toCharArray.foldLeft(EMPTYSTR)((a, b) => {
      tryAndReturnDefaultValue0(Integer.parseInt(b + EMPTYSTR) + random.nextInt(9), random.nextInt(9)) match {
        case x: Int => if (x < 10) a + x else a + random.nextInt(9)
        case _ => a + random.nextInt(9)
      }
    })
  }

  private def cache(): Unit = self.synchronized {
    if (fetchReq.get()) {
      val mx = randomCache.size
      val fetchedResults =
        partSelectorFun(randomCfg.repo, randomCfg.identifier, deIdFields, maxRandomSelects / 6, fac._1, fac._2
        ).map { case (ind, store) => ind -> store.map { case (k, v) => hl7_mappings.getOrElse(k, EMPTYSTR) -> new String(v, UTF8) } }
      fetchedResults.foreach { case (ind, store) => randomCache += mx + ind -> store }
      if (randomCache.size >= maxRandomSelects) fetchReq.set(false)
    }
  }

  private def enoughCache: Boolean = synchronized {
    if (randomCache.size >= maxRandomSelects) true
    else false
  }

  private def getRandomFromStore(key: String, org: String, store: TrieMap[Int, mutable.Map[String, String]] = randomCache, queryAgain: Boolean = false): String = {
    if (identifiers.isDefinedAt(key)) {
      val out = randomIdentifiers(random.nextInt(randomIdentifiers.size)).getOrElse(key, EMPTYSTR)
      if (out != EMPTYSTR) return out
    }
    var out = EMPTYSTR
    for (_ <- 0 until store.size) {
      out = tryAndReturnDefaultValue0(store(random.nextInt(store.size)).getOrElse(key, EMPTYSTR), EMPTYSTR)
      if (out != EMPTYSTR && out != org) return out
    }
    out
  }

  private def getFac: (String, String) = {
    val idx = random.nextInt(facilityRef.length)

    def dft = (facilityRef.head, facilityRef.tail.head)

    tryAndReturnDefaultValue0((facilityRef(idx), facilityRef(random.nextInt(idx, facilityRef.length - idx))), dft)
  }

  private def excludeField(field: String): Boolean = {
    field != obsv_value
    /*(field != obsv_value && !excludes.isDefinedAt(field) && field != "sending_facility" && field != "sending_application"
      && field != "servicing_facility" && !field.contains("enterer_location"))*/
  }

  private def handleText(text: String, field: String, data: String, modifyWith: String = EMPTYSTR)(facility: String, sourceSystem: String): String = {
    var out = text
    if (excludeField(field)) {
      if (valid(text) && valid(data) && data != EMPTYSTR && data.length > 1) {
        out = handleTxtInt(out, field, data, modifyWith, s"\\$caret")(facility, sourceSystem)
        out = handleTxtInt(out, field, data, modifyWith, s"\\&")(facility, sourceSystem)
      }
    }
    out
  }

  private def handleTxtInt(text: String, field: String, data: String, modifyWith: String = EMPTYSTR, delimitedBy: String)(facility: String, sourceSystem: String): String = {
    var out = text
    data.split(delimitedBy, -1).foreach { dt =>
      if (dt != EMPTYSTR && dt.length > 2 && dt != "MSH" && dt != "Final" && dt != "LAB" && dt != "PAT" && dt != "PATH" && dt != "Path" && dt != "SOUT" && dt != "FOUT" && dt!= "The"){
       // && dt != facility && dt != sourceSystem) {
        out = if (dt.contains("(") || dt.contains(")")) {
          tryAndReturnDefaultValue0(StringUtils.replace(out, dt, modifyWith), out)
        } else tryAndReturnDefaultValue0(out replaceAll(dt, modifyWith), out)
      }
    }
    out
  }

  private def handleDates(date: String): (String, Date) = {
    if (valid(date) && date != EMPTYSTR) {
      var formatter = getFormatter(date)
      Try(formatter.parse(date).getTime) match {
        case Success(x) =>
          return (formatter.format(alterDate(new org.joda.time.DateTime(x))), new Date(x))
        case Failure(_) =>
          def tryAgain = {
            val dt = tryAndReturnDefaultValue0(date.substring(0, 8), date)
            formatter = getFormatter(dt)
            val dat = formatter.parse(dt)
            tryAndReturnDefaultValue0((formatter.format(new org.joda.time.DateTime(dat.getTime)), dat), (date, new Date()))
          }

          tryAndReturnDefaultValue0(tryAgain, (EMPTYSTR, new Date()))
      }
    }
    (date, new Date())
  }

  private def getAlternateDateFrmt(date: Date): String = {
    tryAndReturnDefaultValue0(new SimpleDateFormat(format4).format(date), EMPTYSTR)
  }

  private def alterDOB(text: String): String = {
    tryAndReturnDefaultValue0({
      val dob = text.indexOf("DOB")
      val dobPart = text.substring(dob, dob + 20)
      text.replaceAll(dobPart, EMPTYSTR)
    }, text)
  }

  private def alterDate(date: DateTime): Date = {
    date.minusDays(1).minusHours(6).minusMinutes(14).plusSeconds(44).toDate
  }

  private def getFormatter(date: String): SimpleDateFormat = {
    val length = date.length
    if (length == 8) new SimpleDateFormat(format2)
    else if (length > 8 && length <= HL7_ORG.length) new SimpleDateFormat(format1)
    else if (length == format3.length) new SimpleDateFormat(format3)
    else new SimpleDateFormat(format2)
  }

  private def Config(cfg: String): OffHeapConfig = {
    val dest = cfg split "\\&"
    OffHeapConfig(dest(0), dest(2), dest(3).split("\\;", -1).toSet)
  }

  def close(): Unit = {}

  def apply(layout: mutable.LinkedHashMap[String, String], hl7: String): EnrichedData = {
    apply(self.enrichDataPartFun, layout, hl7)
  }

  def repos: Set[String] = Set(deIdCfg.repo, randomCfg.repo, orgCfg.repo)

  private[cdm] object Patterns {
    val onlyNumbersPattern: String => Boolean = "\\d+".r.pattern.matcher(_: String).matches()
    val format1 = HL7_ORG
    val format2 = "yyyyMMdd"
    val format3 = "yyyyMMddHHmmss"
    val format4 = "MM/dd/yyyy"
    val format5 = "MM/dd/yy"
    val format6 = "dd/MM/yyyy"
    val format7 = "MM/dd/yyyy"

  }

}