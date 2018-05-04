package com.hca.cdm

import java.text.SimpleDateFormat
import java.util
import com.hca.cdm.hl7.enrichment.{EnrichDataFromOffHeap, EnrichedData, OffHeapConfig}
import com.hca.cdm.log.Logg
import com.hca.cdm.hl7.constants.HL7Constants._
import scala.collection.mutable
import com.hca.cdm.hl7.model._
import java.util.{Calendar, Collections, Date}
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.atomic.AtomicBoolean
import collection.JavaConverters._
import com.hca.cdm.utils.DateConstants._
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.text.WordUtils
import org.joda.time.DateTime
import scala.collection.concurrent.TrieMap
import scala.util.{Failure, Success, Try}
import Constants._

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
  private val randomCfg = Config(config(0)).getCfg
  private val deIdCfg = Config(config(1)).getCfg
  private val orgCfg = Config(config(2)).getCfg
  private val dataOperators = loadFileAsList(config(3), COMMA, 1).map { case (field, op) => field -> tryAndReturnDefaultValue0(withName(op), DEFAULT) }.toMap
  private val deIdFields = dataOperators.filter(x => x._2 == DE_ID || x._2 == DATE).keySet
  private val deIdFieldsRdm = deIdFields.map(_.replaceAll("\\|", "_"))
  private val fetchReq = new AtomicBoolean(deIdFields.nonEmpty)
  private val facilityRef = loadFileAsList(config(4)).map(_._1.trim).filter(_.startsWith("C")).toArray
  private var facilities = loadFileAsList(config(4)).map(_._1.trim).map(x => x -> generateRandomFacility(x)).toMap
  private val facilityNames = loadFileAsList(config(4)).map(_._1.trim).map(x => x -> x.split(" ", -1).map(_.trim).filter(_.length > 3).map(x => s" $x "))
  private val hl7_mappings = loadFileAsList(config(5)).map { case (k, v) => v -> k }.toMap
  private val phoneNums = dataOperators.filter(x => x._1.contains(cityCode)).map(x => x._1 -> x._2)
  private lazy val fac = getFac
  private val lock = new Object
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
    val facility = layout.getOrElse(sendingFac, EMPTYSTR)
    val messageControlId = layout.getOrElse(controlId, EMPTYSTR)
    if (!facilities.isDefinedAt(facility)) facilities = facilities + Pair(facility, generateRandomFacility(facility))
    val sourceSystem = tryAndReturnDefaultValue0(messageControlId.substring(0, messageControlId.indexOf(underScore)), EMPTYSTR)
    identifiers.foreach { case (k, _) =>
      val out = common(k, org.getOrElse(k, EMPTYSTR), layout, deIdentified.getOrElse(k, getRandomFromStore(k, org.getOrElse(k, EMPTYSTR)))
        , facility, sourceSystem, hl7Mod, obs_note)
      hl7Mod = out.hl7
      obs_note = out.notes
    }
    dataOperators.get(communicationAddress) foreach { op =>
      if (op == ANONYMIZE) {
        layout update(communicationAddress, EMPTYSTR)
        val out = common(communicationAddress, org.getOrElse(communicationAddress, EMPTYSTR), layout, EMPTYSTR, facility, sourceSystem, hl7Mod, obs_note)
        hl7Mod = out.hl7
        obs_note = out.notes
      }
    }
    val temp = handleId(messageControlId, facility, hl7Mod, layout, deIdentified)
    hl7Mod = temp._1
    if (facility != DOT) obs_note = obs_note.replaceAll(facility, facilities.getOrElse(facility, dummyFac))
    dataOperators.foreach {
      case (enrichField, op) =>
        if ((layout isDefinedAt enrichField) && layout(enrichField) != EMPTYSTR && enrichField != patientState && !identifiers.isDefinedAt(enrichField)
          && !phoneNums.isDefinedAt(enrichField)) {
          op match {
            case DE_ID =>
              if ((deIdentified isDefinedAt enrichField) && deIdentified(enrichField) != EMPTYSTR) {
                val out = common(enrichField, org(enrichField), layout, deIdentified(enrichField), facility, sourceSystem, hl7Mod, obs_note)
                hl7Mod = out.hl7
                obs_note = out.notes
              } else {
                val out = common(enrichField, org(enrichField), layout, getRandomFromStore(enrichField, layout(enrichField)), facility, sourceSystem, hl7Mod, obs_note)
                hl7Mod = out.hl7
                obs_note = out.notes
              }
            case ANONYMIZE | DEFAULT =>
              layout update(enrichField, EMPTYSTR)
              hl7Mod = handleText(hl7Mod, enrichField, org(enrichField), layout(enrichField))(facility, sourceSystem)
              obs_note = handleText(obs_note, enrichField, org(enrichField), layout(enrichField))(facility, sourceSystem)
            case DATE =>
              if (enrichField == dob) {

                if (deIdentified.isDefinedAt(dob) && deIdentified(dob) != EMPTYSTR) layout update(enrichField, deIdentified(dob))
                else layout update(enrichField, handleAge(layout(enrichField)))
              } else {
                val dt = handleDates(layout(enrichField))
                layout update(enrichField, dt._1)
                val formattedDt = getAlternateDateFrmt(dt._2)
                hl7Mod = tryAndReturnDefaultValue0(hl7Mod.replaceAll(formattedDt, EMPTYSTR), hl7Mod)
                obs_note = tryAndReturnDefaultValue0(obs_note.replaceAll(formattedDt, EMPTYSTR), obs_note)
              }
              hl7Mod = handleText(hl7Mod, enrichField, org(enrichField), layout(enrichField))(facility, sourceSystem)
              obs_note = handleText(obs_note, enrichField, org(enrichField), layout(enrichField))(facility, sourceSystem)
            case NONE =>
          }
        }
    }

    if ((deIdentified isDefinedAt patientState) && deIdentified(patientState) != EMPTYSTR && org.isDefinedAt(patientState) && deIdentified(patientState) != org(patientState)) {
      val out = common(patientState, org(patientState), layout, deIdentified(patientState), facility, sourceSystem, hl7Mod, obs_note, 1)
      hl7Mod = out.hl7
      obs_note = out.notes
    } else {
      val out = common(patientState, org(patientState), layout, getRandomFromStore(patientState, layout(patientState)), facility, sourceSystem, hl7Mod, obs_note, 1)
      hl7Mod = out.hl7
      obs_note = out.notes
    }
    phoneNums foreach { case (num, _) =>
      layout update(num, EMPTYSTR)
      hl7Mod = handleText(hl7Mod, num, org.getOrElse(num, EMPTYSTR), layout.getOrElse(num, EMPTYSTR))(facility, sourceSystem)
      obs_note = handleText(obs_note, num, org.getOrElse(num, EMPTYSTR), layout.getOrElse(num, EMPTYSTR))(facility, sourceSystem)

    }
    hl7Mod = alterKeywords(hl7Mod)
    obs_note = alterKeywords(obs_note)
    val facilityMod = handleFacilityNames(hl7Mod, obs_note, facility, sourceSystem)
    hl7Mod = facilityMod._1
    obs_note = facilityMod._2
    if (layout isDefinedAt obsv_value) layout update(obsv_value, obs_note)
    layout += controlId -> temp._2
    self.partWriterFun(orgCfg.repo).apply(orgCfg.identifier, orgCfg.fetchKey(org), org, true)
    self.partWriterFun(deIdCfg.repo).apply(deIdCfg.identifier, deIdCfg.fetchKey(org), layout, true)
    EnrichedData(layout, hl7Mod)
  }

  private case class Text(hl7: String, notes: String)

  private def common(enrichField: String, org: String, layout: mutable.LinkedHashMap[String, String], modifyWith: String,
                     facility: String, sourceSystem: String, hl7Mod: String, obs_note: String, length: Int = 2): Text = {

    handleCases(enrichField, org, modifyWith, layout)
    Text(handleText(hl7Mod, enrichField, org, layout(enrichField))(facility, sourceSystem, length)
      , handleText(obs_note, enrichField, org, layout(enrichField))(facility, sourceSystem, length))
  }

  private def handleId(id: String, facility: String, hl7: String, layout: mutable.LinkedHashMap[String, String], deIdentified: mutable.Map[String, String]): (String, String) = {
    val random = if (facility == DOT) id.replaceFirst(s"\\$DOT", dummyFac) else id.replaceAll(facility, facilities.getOrElse(facility, dummyFac))
    if (deIdentified.isDefinedAt(controlId) && deIdentified(controlId) != EMPTYSTR) layout += controlId -> deIdentified(controlId)
    else layout += controlId -> random
    var tempHl7 = hl7
    if (facility == DOT) layout update(sendingFac, dummyFac)
    else layout update(sendingFac, facilities.getOrElse(facility, dummyFac))
    tempHl7 = tryAndReturnDefaultValue0(tempHl7 replaceAll(id, layout(controlId)), tempHl7)
    if (facility != DOT) tempHl7 = tempHl7.replaceAll(facility, facilities.getOrElse(facility, dummyFac))
    (tempHl7, layout(controlId))
  }

  private def generateRandomFacility(fac: String): String = {
    if (fac == DOT) return dummyFac
    val list = new util.ArrayList[Char]()
    fac.toCharArray.sorted.reverse.foreach(x => list.add(x))
    Collections.rotate(list, 2)
    Collections.rotate(list, 3)
    Collections.rotate(list, 4)
    list.asScala.mkString
  }

  private def handleFacilityNames(hl7: String, notes: String, facility: String, sourceSystem: String): (String, String) = {
    var tempHl7 = hl7
    var tempNotes = notes
    facilityNames.foreach {
      case (f, split) =>
        if (skipData(f, facility, sourceSystem)) {
          tempHl7 = tryAndReturnDefaultValue0(tempHl7.replaceAll(f, EMPTYSTR), tempHl7)
          tempNotes = tryAndReturnDefaultValue0(tempNotes.replaceAll(f, EMPTYSTR), tempNotes)
        }
        split.foreach { splits =>
          if (skipData(f, facility, sourceSystem)) {
            tempHl7 = tryAndReturnDefaultValue0(tempHl7.replaceAll(splits, EMPTYSTR), tempHl7)
            tempNotes = tryAndReturnDefaultValue0(tempNotes.replaceAll(splits, EMPTYSTR), tempNotes)
          }
        }
    }
    (tryAndReturnDefaultValue0(tempHl7.replaceAll(email_R, EMPTYSTR), tempHl7), tryAndReturnDefaultValue0(tempNotes.replaceAll(email_R, EMPTYSTR), tempNotes))
  }

  private def handleCases(field: String, org: String, modifyWith: String, layout: mutable.LinkedHashMap[String, String], queryAgain: Boolean = false): Unit = {
    if (field == ssn) layout update(field, handleSsn(modifyWith))
    if (identifiers.isDefinedAt(field)) {
      if (org == EMPTYSTR) layout update(field, org)
      else {
        val temp = handleIdentifiers(modifyWith)
        if (temp == EMPTYSTR && org != EMPTYSTR) layout update(field, random.nextLong(currMillis).toString)
        else layout update(field, temp)
      }
    } else layout update(field, modifyWith)
  }

  private def handleSsn(num: String): String = {
    num.toCharArray.foldLeft(EMPTYSTR)((a, b) => if (b == '-') a + b else {
      tryAndReturnDefaultValue0(Integer.parseInt(b + EMPTYSTR) + random.nextInt(9), random.nextInt(9)) match {
        case x => if (x < 10) a + x else a + b
      }
    })
  }

  private def skipData(fac: String, facility: String, sourceSystem: String): Boolean = {
    (fac != EMPTYSTR && fac.length > 2 && fac != "MSH" && fac != "Final" && fac != "LAB" && fac != "lab" && !fac.equalsIgnoreCase("lab") && fac != "Lab" && fac != "PAT" && fac != "PATH" && fac != "Path" && fac != "SOUT" && fac != "FOUT"
      && fac != facility && fac != sourceSystem && fac != "The" && fac != "EPIC" && fac != "Epic")
  }

  private def handleIdentifiers(num: String): String = {
    num.toCharArray.foldLeft(EMPTYSTR)((a, b) => a + tryAndReturnDefaultValue0(Integer.parseInt(b + EMPTYSTR), EMPTYSTR))
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
    if (randomCache.size >= maxRandomSelects) false
    else true
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

  private def handleText(text: String, field: String, data: String, modifyWith: String = EMPTYSTR)(facility: String, sourceSystem: String, length: Int = 2): String = {
    var out = text
    if (excludeField(field)) {
      if (valid(text) && valid(data) && data != EMPTYSTR && data.length > 1) {
        out = handleTxtInt(out, field, data, modifyWith, s"\\$caret")(facility, sourceSystem, length)
        out = handleTxtInt(out, field, data, modifyWith, s"\\&")(facility, sourceSystem, length)
      }
    }
    out
  }

  private def handleTxtInt(text: String, field: String, data: String, modifyWith: String = EMPTYSTR, delimitedBy: String)(facility: String, sourceSystem: String, length: Int = 2): String = {
    var out = text
    data.split(delimitedBy, -1) foreach { dt =>
      if (dt != EMPTYSTR && dt.length > length && dt != "MSH" && dt != "Final" && dt != "LAB" && dt != "lab" && dt != "PAT"
        && dt != "PATH" && dt != "Path" && dt != "SOUT" && dt != "FOUT" && dt != "The" && dt != facility && dt != sourceSystem &&
        (text contains data)) {
        out = if (dt.contains("(") || dt.contains(")")) tryAndReturnDefaultValue0(StringUtils.replace(out, dt, modifyWith), out)
        else if (length == 1) tryAndReturnDefaultValue0(out replaceFirst(dt, modifyWith), out)
        else tryAndReturnDefaultValue0(out replaceAll(dt, modifyWith), out)
      }
    }
    alternateForms(field, data, modifyWith, out)
  }

  private def alternateForms(field: String, data: String, modifyWith: String, text: String): String = {
    if (field.startsWith(patientName)) {
      var out = text
      out = tryAndReturnDefaultValue0(out.replaceAll(WordUtils.swapCase(data), modifyWith), out)
      out = tryAndReturnDefaultValue0(out.replaceAll(WordUtils.uncapitalize(data), modifyWith), out)
      out = tryAndReturnDefaultValue0(out.replaceAll(WordUtils.capitalize(data), modifyWith), out)
      out = tryAndReturnDefaultValue0(out.replaceAll(WordUtils.capitalizeFully(data), modifyWith), out)
      out
    } else text
  }

  private def handleAge(date: String): String = synchronized {
    if (valid(date) && date != EMPTYSTR) {
      val formatter = getFormatter(date)
      val dtm = new org.joda.time.DateTime(formatter.parse(date).getTime)
      if (new DateTime().year().get() - dtm.year().get() > 89) {
        val cal = Calendar.getInstance()
        cal.set(89, dtm.getMonthOfYear, dtm.getDayOfYear)
        return formatter.format(new org.joda.time.DateTime(cal.getTime).minusMonths(random.nextInt(12)).minusDays(random.nextInt(28)).toDate)
      } else return formatter.format(new org.joda.time.DateTime(formatter.parse(date).getTime).minusMonths(random.nextInt(12)).minusDays(random.nextInt(28)).toDate)
    }
    date
  }

  private def handleDates(date: String): (String, Long) = synchronized {
    if (valid(date) && date != EMPTYSTR) {

      var formatter = getFormatter(date)
      Try(formatter.parse(date).getTime) match {
        case Success(x) =>
          return (getFormatter(date).format(alterDate(new org.joda.time.DateTime(x))), x)
        case Failure(_) =>
          def tryAgain = {
            val dt = date.substring(0, 8)
            formatter = getFormatter(dt)
            val dat = alterDate(new org.joda.time.DateTime(formatter.parse(dt)))
            (getFormatter(dt).format(dat), dat.getTime)
          }

          return tryAndReturnDefaultValue0(tryAgain, default = (date, currMillis))

      }
    }
    (date, currMillis)
  }

  private def getAlternateDateFrmt(time: Long): String = {
    tryAndReturnDefaultValue0(new SimpleDateFormat(format4).format(new Date(time)), EMPTYSTR)
  }

  private def alterKeywords(text: String): String = {
    var out = alterTxt(text, "DOB", 20)
    out = alterTxt(out, "D.O.B.", 20)
    out = alterTxt(out, "AGE/SX", 18)
    out = alterTxt(out, "AGE", 10)
    out = alterTxt(out, "SEX", 10)
    out = alterTxt(out, "SX", 10)
    for (_ <- 0 until 3) {
      out = alterTxt(out, "Signed", 80)
    }
    for (_ <- 0 until 3) {
      out = alterTxt(out, "signed", 30)
    }
    out
  }

  private def alterTxt(text: String, keyword: String, numOfChars: Int): String = {
    tryAndReturnDefaultValue0({
      val idx = text.indexOf(keyword)
      val part = text.substring(idx, idx + numOfChars)
      StringUtils.replace(text, part, EMPTYSTR)
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


  def close(): Unit = {}

  def apply(layout: mutable.LinkedHashMap[String, String], hl7: String): EnrichedData = {
    apply(self.enrichDataPartFun, layout, hl7)
  }

  def repos: Set[String] = Set(deIdCfg.repo, randomCfg.repo, orgCfg.repo)


}

private[cdm] object Patterns {
  val onlyNumbersPattern: String => Boolean = "\\d+".r.pattern.matcher(_: String).matches()
  val format1: String = HL7_ORG
  val format2: String = "yyyyMMdd"
  val format3: String = "yyyyMMddHHmmss"
  val format4: String = "MM/dd/yyyy"
  val format5: String = "MM/dd/yy"
  val format6: String = "dd/MM/yyyy"
  val format7: String = "MM/dd/yyyy"

}

private[cdm] object Constants {
  val email_R = "[a-zA-Z0-9.-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z0-9.-]+"
  val obsv_value: String = "obsv_value"
  val ssn: String = "ssn_num_patient"
  val identifiers: Map[String, String] = Set("medical_record_num", "medical_record_urn", "patient_account_num").map(x => x -> x).toMap
  val patientState: String = "patient_address|state_or_province"
  val communicationAddress: String = "phone_num_home|communication_address"
  val patientName: String = "patient_name"
  val sendingFac: String = "sending_facility"
  val controlId: String = "message_control_id"
  val dob: String = "date_time_of_birth"
  val dummyFac: String = "NOFAC"
  val cityCode: String = "area_city_code"
}

private[cdm] case class Config(private val cfg: String) {
  private val dest = cfg split "\\&"

  def getCfg: OffHeapConfig = OffHeapConfig(dest(0), dest(2), dest(3).split("\\;", -1).toSet)
}
