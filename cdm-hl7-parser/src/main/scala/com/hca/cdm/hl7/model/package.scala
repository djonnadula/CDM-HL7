package com.hca.cdm.hl7

import java.io.ByteArrayOutputStream
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature.WRITE_NULL_MAP_VALUES
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.hca.cdm.Models.MSGMeta
import com.hca.cdm._
import com.hca.cdm.exception.CdmException
import com.hca.cdm.hl7.constants.HL7Constants._
import com.hca.cdm.hl7.constants.HL7Types
import com.hca.cdm.hl7.constants.HL7Types.{withName => whichHl7}
import com.hca.cdm.hl7.constants.HL7Types.{HL7, UNKNOWN}
import com.hca.cdm.hl7.enrichment.{EnrichData, EnrichDataFromOffHeap, NoEnricher}
import com.hca.cdm.hl7.model.SegmentsState.SegState
import com.hca.cdm.hl7.model.Destinations.Destination
import com.hca.cdm.log.Logg
import com.hca.cdm.utils.DateUtil.{currentTimeStamp => timeStamp}
import com.hca.cdm.utils.Filters.Conditions.{withName => matchCriteria}
import com.hca.cdm.utils.Filters.Expressions.{withName => relationWithNextFilter}
import com.hca.cdm.utils.Filters.MultiValues.{withName => multiValueRange}
import com.hca.cdm.utils.Filters.FILTER
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData.Record
import java.util.regex.Pattern
import org.apache.avro.generic._
import org.apache.avro.io._
import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

/**
  * Created by Devaraj Jonnadula on 8/22/2016.
  *
  * Package with Utilities for Loading Templates, Segments ...
  */
package object model extends Logg {

  lazy val MSH_Segment = "0001.MSH"
  lazy val Message_Type_Segment = "message_type"
  lazy val Message_Code = "message_code"
  lazy val Message_Version = "version_id"
  lazy val Control_Id_key = "message_control_id"
  lazy val sending_Facility = "sending_facility"
  lazy val Msg_Type_Hier = Seq(MSH_Segment, Message_Type_Segment, Message_Code)
  lazy val Observation_Col = "obsv_value"
  lazy val MSH_INDEX = "0001.MSH"
  lazy val commonNodeStr = "0000.COMN"
  lazy val OBX_SEG = "OBX"
  lazy val commonSegkey = "seg"
  lazy val notValidStr = "Not Valid Input"
  lazy val skippedStr = "SKIPPED"
  lazy val filteredStr = "FILTERED"
  lazy val timeStampKey = "etl_firstinsert_datetime"
  lazy val fieldSeqNum = "field_seq_num"
  lazy val NA = "Not Applicable"
  private lazy val toJson = jsonHandler()
  private lazy val NONE = MSGMeta("no message control ID", "no time from message", EMPTYSTR, EMPTYSTR, EMPTYSTR, "no Sending Facility")
  lazy val caret = "^"
  lazy val DOT = "."
  lazy val EQUAL = "="
  lazy val ESCAPE = "\\"
  lazy val segmentSequence = "segment_sequence"
  lazy val commonNode: mutable.LinkedHashMap[String, String] = synchronized {
    val temp = new mutable.LinkedHashMap[String, String]
    val nodeEle = lookUpProp("common.elements") match {
      case EMPTYSTR => throw new CdmException("No Common Elements found. ")
      case valid => valid split("\\^", -1)
    }
    nodeEle.foreach(ele => temp += ele -> EMPTYSTR)
    temp
  }

  lazy val MSHMappings: mutable.HashMap[String, Array[(String, String, String, String)]] = synchronized(
    commonSegmentMappings(lookUpProp("common.elements.msh.mappings")))
  lazy val PIDMappings: mutable.HashMap[String, Array[(String, String, String, String)]] = synchronized(
    commonSegmentMappings(lookUpProp("common.elements.pid.mappings")))
  private val DUMMY_CONTAINER = new mutable.LinkedHashMap[String, Any]

  def hl7Type(data: mapType): HL7 = {
    Try(data.getOrElse(MSH_Segment, DUMMY_CONTAINER).asInstanceOf[mapType].getOrElse(Message_Type_Segment, DUMMY_CONTAINER).asInstanceOf[mapType].getOrElse(Message_Code, "UNKNOWN")) match {
      case Success(any) => whichHl7(any.asInstanceOf[String])
      case Failure(_) => UNKNOWN
    }
  }

  private case class RejectSchema(processName: String = "process_name", controlID: String = "message_control_id",
                                  tranTime: String = "msg_create_date_time",
                                  mrn: String = "patient_mrn", urn: String = "patient_urn",
                                  accntNum: String = "patient_account_number",
                                  rejectReason: String = "reject_reason",
                                  rejectData: String = "rejected_message_data",
                                  etlTime: String = "etl_firstinsert_datetime")

  private val rejectSchemaMapping = RejectSchema()
  private val rejectSchema = {
    val temp = new mutable.LinkedHashMap[String, Any]
    rejectSchemaMapping.productIterator.foreach(sch => temp += sch.asInstanceOf[String] -> EMPTYSTR)
    temp
  }

  def jsonHandler(filterNulls: Boolean = true): (Any) => String = {
    val mapper = new ObjectMapper().registerModule(DefaultScalaModule)
    if (filterNulls) mapper disable WRITE_NULL_MAP_VALUES
    mapper.writer.writeValueAsString(_)
  }


  private def getRejectSchema = rejectSchema.clone().transform((k, v) => EMPTYSTR)

  private case object RejectAvroSchema {
    private lazy val schema: Schema = new Schema.Parser().parse(readFile("HL7_Reject.avro").getLines().mkString(EMPTYSTR))
    private lazy val encoder = EncoderFactory.get
    private lazy val writer: GenericDatumWriter[AnyRef] = new GenericDatumWriter(schema)

    def avroRejectRecord: Record = new Record(schema)

    def encoderFac: EncoderFactory = encoder

    def datumWriter: GenericDatumWriter[AnyRef] = writer
  }


  def commonSegmentMappings(mappingData: String): mutable.HashMap[String, Array[(String, String, String, String)]] = {
    val temp = new mutable.HashMap[String, Array[(String, String, String, String)]]
    val nodeEle = mappingData match {
      case EMPTYSTR => throw new CdmException("No Common Elements Sub Components Mappings found. ")
      case valid => valid split("\\^", -1)
    }
    nodeEle.foreach(ele => {
      val subComp = ele.split(COMMA, -1)
      temp += subComp.head -> subComp.map(x => {
        if (x contains PIPE_DELIMITED_STR) {
          val split = x split("\\" + PIPE_DELIMITED_STR, -1)
          val temp = split(1)
          if (temp contains AMPERSAND) {
            val subCompSplit = temp split("\\" + AMPERSAND, -1)
            val kv = subCompSplit(1) split("\\" + EQUAL, -1)
            (split(0), subCompSplit(0), kv(0), kv(1))
          } else (split(0), temp, EMPTYSTR, EMPTYSTR)
        } else (x, EMPTYSTR, EMPTYSTR, EMPTYSTR)
      })

    })
    temp
  }

  object HL7State extends Enumeration {

    type hl7State = Value
    val PROCESSED = Value("PROCESSED")
    val FAILED = Value("FAILED")
    val REJECTED = Value("REJECTED")
    val OVERSIZED = Value("OVERSIZED")
    val UNKNOWNMAPPING = Value("UNKNOWNMAPPING")
  }

  object SegmentsState extends Enumeration {

    type SegState = Value
    val SKIPPED = Value("SKIPPED")
    val PROCESSED = Value("PROCESSED")
    val FAILED = Value("FAILED")
    val INVALID = Value("INVALID")
    val NOTAPPLICABLE = Value("N/A")
    val OVERSIZED = Value("OVERSIZED")
    val FILTERED = Value("FILTERED")

  }

  object OutFormats extends Enumeration {

    type OutFormat = Value
    val JSON = Value("JSON")
    val DELIMITED = Value("DELIMITED")
    val AVRO = Value("AVRO")
    val RAWHL7 = Value("RAWHL7")

  }

  object Destinations extends Enumeration {
    type Destination = Value
    val KAFKA = Value("KAFKA")
    val WSMQ = Value("WSMQ")
    val DEFAULT = Value("KAFKA")
    val HBASE = Value("HBASE")
  }

  def applyAvroData(container: Record, key: String, value: AnyRef): Unit = {
    tryAndLogErrorMes(container.put(key, value), error(_: Throwable), Some(s"Cannot insert key $key  into Avro Schema ${container.getSchema}"))
  }


  import OutFormats._

  def rejectMsg(hl7: String, stage: String = EMPTYSTR, meta: MSGMeta, reason: String, data: mapType, t: Throwable = null, raw: String = null, stack: Boolean = true, format: OutFormat = DELIMITED): AnyRef = {
    format match {
      case JSON =>
        val rejectRecord = getRejectSchema
        import rejectSchemaMapping._
        rejectRecord update(processName, s"$hl7$COLON$stage")
        rejectRecord update(controlID, meta.controlId)
        rejectRecord update(tranTime, meta.msgCreateTime)
        rejectRecord update(mrn, meta.medical_record_num)
        rejectRecord update(urn, meta.medical_record_urn)
        rejectRecord update(accntNum, meta.account_num)
        rejectRecord update(rejectReason, if (t != null) reason + (if (stack) t.getStackTrace mkString caret) else reason)
        rejectRecord update(rejectData, if (raw ne null) raw else if (data != null) data else EMPTYSTR)
        rejectRecord update(etlTime, timeStamp)
        toJson(rejectRecord)
      case DELIMITED =>
        s"$hl7$COLON$stage$PIPE_DELIMITED_STR${meta.controlId}$PIPE_DELIMITED_STR${meta.msgCreateTime}$PIPE_DELIMITED_STR${meta.medical_record_num}" +
          s"$PIPE_DELIMITED_STR${meta.medical_record_urn}$PIPE_DELIMITED_STR${meta.account_num}$PIPE_DELIMITED_STR" + timeStamp + PIPE_DELIMITED_STR +
          (if (t != null) reason + (if (stack) t.getStackTrace mkString caret) else reason) + PIPE_DELIMITED_STR + (if (raw ne null) raw else toJson(data))
      case AVRO =>
        import rejectSchemaMapping._
        import RejectAvroSchema._
        val rejectRecord = avroRejectRecord
        applyAvroData(rejectRecord, processName, s"$hl7$COLON$stage")
        applyAvroData(rejectRecord, controlID, meta.controlId)
        applyAvroData(rejectRecord, tranTime, meta.msgCreateTime)
        applyAvroData(rejectRecord, mrn, meta.medical_record_num)
        applyAvroData(rejectRecord, urn, meta.medical_record_urn)
        applyAvroData(rejectRecord, accntNum, meta.account_num)
        applyAvroData(rejectRecord, rejectReason, if (t != null) reason + (if (stack) t.getStackTrace mkString caret) else reason)
        applyAvroData(rejectRecord, rejectData, if (raw ne null) raw else if (data != null) data else EMPTYSTR)
        applyAvroData(rejectRecord, etlTime, timeStamp)
        val stream = new ByteArrayOutputStream(256)
        val encoder = encoderFac.directBinaryEncoder(stream, null)
        datumWriter.write(rejectRecord, encoder)
        encoder flush()
        stream toByteArray
      case _ => throw new CdmException(s"Format $format for Reject not yet Supported")
    }
  }

  def rejectRawMsg(hl7: String, stage: String = EMPTYSTR, raw: String, reason: String, t: Throwable, stackTrace: Boolean = true): AnyRef = {
    rejectMsg(hl7, stage, metaFromRaw(raw), reason, null, t, raw, stackTrace)
  }

  def metaFromRaw(raw: String): MSGMeta = {
    val delim = if (raw contains "\r\n") "\r\n" else "\n"
    val rawSplit = raw split delim
    if (valid(rawSplit)) {
      val msh = rawSplit(0)
      val tempMsh = PIPER split msh
      val tempPid = PIPER split findSeg(rawSplit, PID)
      Try(MSGMeta(tempMsh(9), tempMsh(6), tryAndReturnDefaultValue(asFunc(dataAtIndex(tempPid)(3)), EMPTYSTR), tryAndReturnDefaultValue(asFunc(dataAtIndex(tempPid)(4)), EMPTYSTR),
        tryAndReturnDefaultValue(asFunc(dataAtIndex(tempPid)(18, 1)), EMPTYSTR), tryAndReturnDefaultValue(asFunc(dataAtIndex(tempMsh)(3, 1)), EMPTYSTR),
        tryAndReturnDefaultValue(asFunc(dataAtIndex(tempMsh)(8, 1)), EMPTYSTR))) match {
        case Success(me) => me
        case _ => NONE
      }
    } else NONE
  }

  private def dataAtIndex(segment: Array[String], delim: String = "\\" + caret)(firstIndex: Int, secondaryIndex: Int = 0): String = {
    if (valid(segment, firstIndex)) {
      if (segment(firstIndex) contains caret) (segment(firstIndex) split delim) (secondaryIndex)
      else segment(firstIndex)
    } else EMPTYSTR
  }

  private def findSeg(rawSplit: Array[String], seg: String): String = {
    if (valid(rawSplit)) rawSplit.foreach(segment => if (segment.startsWith(seg)) return segment)
    EMPTYSTR
  }

  def initSegStateWithZero: mutable.HashMap[SegState, Long] = {
    val temp = new mutable.HashMap[SegState, Long]()
    SegmentsState.values.foreach(x => temp += x -> 0L)
    temp
  }

  private[model] def loadEtlConfig(noOps: Boolean, request: String): String = {
    if (!noOps) return EMPTYSTR
    val config = loadConfig(lookUpProp("hl7.adhoc-etl"))
    EnrichCacheManager().cache(request, FieldsTransformer(
      FieldSelector(config.getOrDefault(s"$request$DOT${"fields.selection"}", EMPTYSTR).asInstanceOf[String]),
      FieldsCombiner(config.getOrDefault(s"$request$DOT${"fields.combine"}", EMPTYSTR).asInstanceOf[String]),
      FieldsValidator(config.getOrDefault(s"$request$DOT${"fields.validate"}", EMPTYSTR).asInstanceOf[String]),
      FieldsStaticOperator(config.getOrDefault(s"$request$DOT${"fields.static"}", EMPTYSTR).asInstanceOf[String]), {
        val impl = config.getOrDefault(s"$request$DOT${"reference.handle"}", EMPTYSTR).asInstanceOf[String]
        if (impl != EMPTYSTR) {
          tryAndThrow(currThread.getContextClassLoader.loadClass(impl).getConstructor(classOf[Array[String]]).newInstance(
            config.getOrDefault(s"$request$DOT${"reference.props"}", EMPTYSTR).asInstanceOf[String].split(COMMA)).asInstanceOf[EnrichData], error(_: Throwable)
            , Some(s"Impl for $impl cannot be initiated"))
        } else NoEnricher()
      }, {
        val impl = config.getOrDefault(s"$request$DOT${"reference.offheap.handle"}", EMPTYSTR).asInstanceOf[String]
        if (impl != EMPTYSTR) {
          tryAndThrow(currThread.getContextClassLoader.loadClass(impl).getConstructor(classOf[Array[String]]).newInstance(
            config.getOrDefault(s"$request$DOT${"reference.offheap.props"}", EMPTYSTR).asInstanceOf[String].split(COMMA)).asInstanceOf[EnrichDataFromOffHeap], error(_: Throwable)
            , Some(s"Impl for $impl cannot be initiated"))
        } else NoEnricher()
      }
    ))
    request
  }

  def segmentsForHl7Type(msgType: HL7, segments: List[(String, String)], delimitedBy: String = s"$ESCAPE$caret", modelFieldDelim: String = PIPE_DELIMITED_STR): Hl7Segments = {
    import OutFormats._
    Hl7Segments(msgType, segments flatMap (seg => {
      if (seg._1 contains "ADHOC") {
        val empty = new mutable.LinkedHashSet[(String, String)]
        val adhoc = seg._1 split COLON
        if (valid(adhoc, 3)) {
          def access(index: Int, store: Array[String] = adhoc) = () => store(index)

          val filterFile = tryAndReturnDefaultValue(access(4), EMPTYSTR)
          val fieldWithNoAppends = tryAndReturnDefaultValue(access(5), EMPTYSTR).split("\\&", -1)
          val tlmAckApplication = tryAndReturnDefaultValue(access(6), EMPTYSTR)
          val transformationsReq = tryAndReturnDefaultValue(access(7), EMPTYSTR).split("\\&")
          val etlTransformations = valid(transformationsReq) && transformationsReq(0) == "TRANSFORMATIONS"
          val etlTransMultiReq = if (tryAndReturnDefaultValue(access(1, transformationsReq), EMPTYSTR) != EMPTYSTR) {
            s"$DOT${tryAndReturnDefaultValue(access(1, transformationsReq), EMPTYSTR)}"
          } else {
            tryAndReturnDefaultValue(access(1, transformationsReq), EMPTYSTR)
          }
          val segStruct = adhoc take 2 mkString COLON
          val outFormats = adhoc(2) split "\\^"
          val outDest = adhoc(3) split "\\^"
          var index = -1
          outFormats.map(outFormat => {
            index += 1
            val dest = outDest(index) split "\\&"
            val destSys = tryAndReturnDefaultValue(asFunc(Destinations.withName(dest(1))), Destinations.KAFKA)
            val hBaseConfig = if (destSys == Destinations.HBASE) {
              val keys = new ListBuffer[String]
              dest(3).split(SEMICOLUMN, -1).foreach(keys += _)
              Some(HBaseConfig(dest(2), keys))
            } else None
            val outFormSplit = outFormat split AMPERSAND
            if (outFormat contains RAWHL7.toString) {
              (s"$segStruct$COLON${outFormSplit(0)}", ADHOC(RAWHL7,
                DestinationSystem(destSys, dest(0), hBaseConfig), empty, fieldWithNoAppends,
                tlmAckApplication, loadEtlConfig(etlTransformations, s"${adhoc(0)}$DOT${adhoc(1)}$DOT$DELIMITED$etlTransMultiReq")), loadFilters(filterFile))
            }
            else if (outFormat contains JSON.toString) {
              (s"$segStruct$COLON${outFormSplit(0)}", ADHOC(JSON,
                DestinationSystem(destSys, dest(0), hBaseConfig), loadFileAsList(outFormSplit(1)),
                fieldWithNoAppends, tlmAckApplication, loadEtlConfig(etlTransformations, s"${msgType.toString}$DOT${adhoc(0)}$DOT${adhoc(1)}$DOT$JSON$etlTransMultiReq")), loadFilters(filterFile))
            } else {
              (s"$segStruct$COLON${outFormSplit(0)}", ADHOC(DELIMITED,
                DestinationSystem(destSys, dest(0), hBaseConfig),
                tryAndReturnDefaultValue(asFunc(loadFileAsList(outFormSplit(1))), empty), fieldWithNoAppends,
                tlmAckApplication, loadEtlConfig(etlTransformations, s"${msgType.toString}$DOT${adhoc(0)}$DOT${adhoc(1)}$DOT$DELIMITED$etlTransMultiReq")),
                loadFilters(tryAndReturnDefaultValue(asFunc(filterFile), EMPTYSTR)))
            }
          }).map(ad => Model(ad._1, seg._2, delimitedBy, modelFieldDelim, Some(ad._2), Some(ad._3))).toList
        } else throw new DataModelException("ADHOC Meta cannot be accepted. Please Check it " + seg._1)
      } else {
        List(Model(seg._1, seg._2, delimitedBy, modelFieldDelim))
      }
    }) groupBy (_.reqSeg))
  }

  case class Hl7Segments(msgType: HL7, models: Map[String, List[Model]])

  case class HL7TransRec(rec: Either[(String, mutable.LinkedHashMap[String, Any], MSGMeta), Throwable])

  case class Hl7SegmentTrans(trans: Either[Traversable[(String, Throwable)], String])

  case class DestinationSystem(system: Destination = Destinations.KAFKA, route: String, offHeapConfig: Option[HBaseConfig] = None)

  case class HBaseConfig(family: String, key: ListBuffer[String])

  case class FieldsTransformer(selector: FieldSelector, aggregator: FieldsCombiner, validator: FieldsValidator, staticOperator: FieldsStaticOperator,
                               dataEnRicher: EnrichData, offHeapDataEnricher: EnrichDataFromOffHeap) {
    private val offHeapManager: TrieMap[String, (Any) => Any] = new TrieMap()

    def applyTransformations(data: mutable.LinkedHashMap[String, String]): Unit = {
      selector apply data
      aggregator apply data
      staticOperator apply data
      validator apply data
      dataEnRicher apply data
      offHeapDataEnricher apply data
      // tryAndFallbackTo(asFunc(offHeapDataEnricher apply data), offHeapDataEnricher apply(null, data))
    }
  }

  case class ADHOC(outFormat: OutFormat, destination: DestinationSystem, outKeyNames: mutable.LinkedHashSet[(String, String)]
                   , reqNoAppends: Array[String] = Array.empty[String],
                   ackApplication: String = EMPTYSTR, transformer: String) {
    val multiColumnLookUp: Map[String, Map[String, String]] = outKeyNames.groupBy(_._2).filter(_._2.size > 1).
      map(multi => multi._1 -> multi._2.map(ele => ele._1 -> EMPTYSTR).toMap)
  }

  private[model] case class FieldSelector(selectFieldsCriteria: String = EMPTYSTR) {
    private lazy val selectCriteria: List[(String, String, String, String, String)] = if (selectFieldsCriteria != EMPTYSTR)
      selectFieldsCriteria.split(COMMA).toList.map {
        x =>
          val temp = x.split(COLON)
          (temp(0).split(AMPERSAND)(0), temp(0).split(AMPERSAND)(1), temp(1), temp(2), tryAndReturnDefaultValue(asFunc(temp(3)), "DELETE"))
      }
    else Nil

    def apply(layout: mutable.LinkedHashMap[String, String]): Unit = {
      selectCriteria.foreach {
        case (lookUpField, criteria, selectFrom, modify, op) =>
          if ((layout isDefinedAt lookUpField) && (layout isDefinedAt selectFrom) && (layout isDefinedAt modify)) {
            layout(lookUpField).split("\\" + caret)
              .view.zipWithIndex.foreach { dataPoint =>
              if (dataPoint._1 == criteria) {
                layout update(modify, tryAndReturnDefaultValue(asFunc(layout(selectFrom).split("\\" + caret)(dataPoint._2)), EMPTYSTR))
              }
            }
            if (op == "DELETE") {
              layout remove lookUpField
              layout remove selectFrom
            }
          }
      }
    }
  }

  private[model] case class FieldsCombiner(combineFields: String = EMPTYSTR) {
    private lazy val fieldsToCombine: List[(Array[String], String, String, String)] = if (combineFields != EMPTYSTR)
      combineFields.split(COMMA).toList.map {
        x =>
          val split = x.split(COLON)
          val temp = split.splitAt(split.length - 3)
          (temp._1(0).split(AMPERSAND), temp._2(0), temp._2(1),
            tryAndReturnDefaultValue(asFunc(temp._2(2)), "DELETE"))
      }
    else Nil

    def apply(layout: mutable.LinkedHashMap[String, String]): Unit = {
      fieldsToCombine.foreach {
        case (criteria, modify, delimitedBy, action) =>
          layout update(modify, criteria.map(field => layout.getOrElse(field, EMPTYSTR)).mkString(delimitedBy))
          action match {
            case "KEEP" =>
            case "DELETE" => criteria foreach (layout remove)
            case any => throw new DataModelException(s" $any operation is not supported.")
          }
      }
    }
  }

  private[model] case class FieldsValidator(validateFields: String = EMPTYSTR) {
    private lazy val fieldsToValidate: List[(String, (CharSequence) => Boolean, String)] = if (validateFields != EMPTYSTR)
      validateFields.split(COMMA).toList.map {
        x =>
          val temp = x.split(COLON, -1)
          (temp(0), Pattern.compile(temp(1), Pattern.CASE_INSENSITIVE + Pattern.LITERAL).matcher(_: CharSequence).find()
            , tryAndReturnDefaultValue(asFunc(temp(2)), EMPTYSTR))
      }
    else Nil

    def apply(layout: mutable.LinkedHashMap[String, String]): Unit = {
      fieldsToValidate.foreach {
        case (criteria, check, replaceWith) =>
          if (check(layout.getOrElse(criteria, EMPTYSTR)))
            layout update(criteria, replaceWith)
      }
    }
  }

  private[model] case class FieldsStaticOperator(fieldsWithStaticOp: String = EMPTYSTR) {
    private lazy val staticFields: List[(String, String)] = if (fieldsWithStaticOp != EMPTYSTR)
      fieldsWithStaticOp.split(caret).toList.map {
        x =>
          val temp = x.split(COLON)
          (temp(0), temp(1))
      }
    else Nil

    def apply(layout: mutable.LinkedHashMap[String, String]): Unit = {
      staticFields.foreach {
        case (criteria, updateWith) =>
          if (layout isDefinedAt criteria) layout update(criteria, updateWith)
      }
    }
  }

  case class Model(reqSeg: String, segStr: String, delimitedBy: String = s"$ESCAPE$caret", modelFieldDelim: String = PIPE_DELIMITED_STR,
                   adhoc: Option[ADHOC] = None, filters: Option[Array[FILTER]] = None) extends modelLayout {
    lazy val modelFilter: Map[String, mutable.Set[String]] = segFilter(segStr, delimitedBy, modelFieldDelim)
    lazy val EMPTY: mutable.LinkedHashMap[String, String] = mutable.LinkedHashMap.empty[String, String]

    override def getLayout: mutable.LinkedHashMap[String, String] = modelLayout(segStr, delimitedBy, modelFieldDelim, adhoc.isDefined)

    def layoutCopy: mutable.LinkedHashMap[String, String] = cachedLayout.clone.transform((k, v) => if ((k ne commonSegkey) & (v ne EMPTYSTR)) EMPTYSTR else v)

    def adhocLayout(layout: mutable.LinkedHashMap[String, String], keyNames: mutable.LinkedHashSet[(String, String)],
                    multiColumnLookUp: Map[String, Map[String, String]]): mutable.LinkedHashMap[String, String] = {
      val store = new mutable.LinkedHashMap[String, String]
      keyNames.foreach { case (k, v) =>
        if (exists(multiColumnLookUp, v) && !(store isDefinedAt v)) store += v -> getDataFromMultiLocations(multiColumnLookUp(v), layout)
        else if (!(store isDefinedAt v)) store += v -> layout(k)
        else store update(v, store(v) + layout(k))
      }
      store
    }

    private def getDataFromMultiLocations(possibleLocations: Map[String, String], layout: mutable.LinkedHashMap[String, String]): String = {
      possibleLocations.foreach(ele => if ((layout isDefinedAt ele._1) && layout(ele._1) != EMPTYSTR) return layout(ele._1))
      EMPTYSTR

    }
  }

  private[model] sealed trait modelLayout {
    protected lazy val cachedLayout: mutable.LinkedHashMap[String, String] = getLayout

    def getLayout: mutable.LinkedHashMap[String, String]

  }

  case class MsgTypeMeta(msgType: com.hca.cdm.hl7.constants.HL7Types.HL7, kafka: String)

  case class ReceiverMeta(msgType: com.hca.cdm.hl7.constants.HL7Types.HL7, wsmq: Set[String], kafka: String)

  def loadSegments(segments: String, delimitedBy: String = COMMA): Map[String, List[(String, String)]] = {
    if (segments == EMPTYSTR) return Map.empty[String, List[(String, String)]]
    val reader = readFile(segments).bufferedReader()
    val temp = Stream.continually(reader.readLine()).takeWhile(valid(_)).toList.map(seg => {
      val splits = seg split(delimitedBy, -1)
      if (valid(splits, 3)) {
        splits(0) -> (splits(1), splits(2))
      } else {
        EMPTYSTR -> (EMPTYSTR, EMPTYSTR)
      }
    }).takeWhile(_._1 ne EMPTYSTR).groupBy(_._1).map({ case (k, v) => k -> v.map(x => x._2) })
    closeResource(reader)
    temp
  }

  def applySegmentsToAll(template: Map[String, List[(String, String)]], messageTypes: Array[String]): Map[String, List[(String, String)]] = {
    val temp = new mutable.HashMap[String, List[(String, String)]]() ++ template
    val allSegments = temp.getOrElse("ALL", new Array[(String, String)](0).toList)
    if (allSegments.nonEmpty) temp -= "ALL"
    messageTypes.foreach(hl7 => {
      if (temp isDefinedAt hl7) temp update(hl7, temp(hl7) ::: allSegments)
      else temp += hl7 -> allSegments
    })
    temp.toMap
  }

  def loadFilters(file: String, delimitedBy: String = COMMA): Array[FILTER] = {
    file match {
      case EMPTYSTR => Array.empty[FILTER]
      case _ =>
        readFile(file).getLines().takeWhile(valid(_)).map(temp => temp split delimitedBy) filter (valid(_, 5)) map {
          x =>
            if (x.length == 5) FILTER(x(0), (x(1), x(2)), (matchCriteria(x(3)), relationWithNextFilter(x(4))), None)
            else if (x.length == 6) FILTER(x(0), (x(1), x(5)), (matchCriteria(x(3)),
              relationWithNextFilter(x(4))), Some(multiValueRange(x(2))))
            else throw new DataModelException(s"Invalid Config for Filter Defined $x")
        } toArray

    }
  }

  def Reassignments(file: String): (Map[String, String], Map[String, Map[String, String]], Map[String, mutable.LinkedHashMap[String, Any]]) = {
    val reassignMeta = new mutable.HashMap[String, String]()
    val reassignStruct = new mutable.HashMap[String, mutable.LinkedHashMap[String, Any]]
    val reassignmentMapping = new mutable.HashMap[String, Map[String, String]]
    readFile(file).getLines().takeWhile(valid(_)).map(temp => temp split(COMMA, -1)) takeWhile (valid(_)) foreach {
      case data@ele if data.nonEmpty =>
        val reassignStructTemp = new mutable.LinkedHashMap[String, Any]
        val reassignmentMappingTemp = new mutable.HashMap[String, String]
        reassignMeta += data(0) -> EMPTYSTR
        reassignMeta += data(1) -> EMPTYSTR
        reassignMeta += data(2) -> EMPTYSTR
        val reassignData = data.takeRight(data.length - 3)
        reassignData.foreach(x => {
          if (x != EMPTYSTR) {
            val rData = x.split("\\" + caret, -1)
            val structData = PIPER split rData(1)
            val struct = new mutable.LinkedHashMap[String, String]
            structData.tail.map(x => struct += x -> EMPTYSTR)
            if (struct isEmpty) reassignStructTemp += structData(0) -> EMPTYSTR
            else reassignStructTemp += structData(0) -> struct
            reassignmentMappingTemp += rData(0) -> structData(0)
          }
        })
        reassignmentMapping += s"${data.apply(0)}${data.apply(1)}${data.apply(2)}" -> reassignmentMappingTemp.toMap
        reassignStruct += s"${data.apply(0)}${data.apply(1)}${data.apply(2)}" -> reassignStructTemp
    }
    (reassignMeta toMap, reassignmentMapping toMap, reassignStruct toMap)
  }

  def loadFile(file: String, delimitedBy: String = EQUAL, keyIndex: Int = 0): Map[String, String] = {
    readFile(file).getLines().takeWhile(valid(_)).map(temp => temp split delimitedBy) takeWhile (valid(_)) map {
      case x@ele if ele.nonEmpty => ele(keyIndex) -> x(keyIndex + 1)
    } toMap
  }

  def loadFileAsList(file: String, delimitedBy: String = COMMA, keyIndex: Int = 0): mutable.LinkedHashSet[(String, String)] = {
    val store = new mutable.LinkedHashSet[(String, String)]()
    readFile(file).getLines().filter(valid(_)).filter(_ != EMPTYSTR).foreach(temp => {
      val splitD = temp split delimitedBy
      if (splitD.nonEmpty) store += ((splitD(keyIndex), splitD(keyIndex + 1)))
    })
    store
  }

  def loadTemplate(template: String = "templateinfo.properties", delimitedBy: String = COMMA): Map[String, Map[String, Array[String]]] = {
    loadFile(template).map(file => {
      val reader = readFile(file._2).bufferedReader()
      val temp = Stream.continually(reader.readLine()).takeWhile(valid(_)).toList map (x => x split(delimitedBy, -1)) takeWhile (valid(_)) map (splits => {
        splits.head -> splits.tail
      })
      closeResource(reader)
      file._1 -> temp.toMap
    })
  }

  def getMsgTypeMeta(msgType: HL7, sourceIn: String): MsgTypeMeta = MsgTypeMeta(msgType, sourceIn)

  def getReceiverMeta(msgType: HL7, sourceIn: String, out: String): ReceiverMeta =
    ReceiverMeta(msgType, sourceIn.split(COMMA, -1).toSet, out)

  def handleAnyRef(data: AnyRef): String = {
    data match {
      case map: mapType => map.values mkString
      case list: listType => list map (map => map.values mkString) mkString
      case s: String => s
    }
  }

  private def segFilter(segmentData: String, delimitedBy: String, modelFieldDelim: String): Map[String, mutable.Set[String]] = {
    val temp = new mutable.HashMap[String, mutable.Set[String]] with mutable.MultiMap[String, String]
    (segmentData split(delimitedBy, -1)).filter(_ != EMPTYSTR) foreach (ele => {
      if (ele contains modelFieldDelim) {
        val repeats = ele split ("\\" + modelFieldDelim)
        if (repeats.length >= 2) {
          val k = repeats.head
          repeats.tail.foreach(v => temp addBinding(k, v))
        } else {
          temp addBinding(ele, EMPTYSTR)
        }
      } else temp addBinding(ele, EMPTYSTR)
    })
    temp.toMap

  }


  private def modelLayout(segmentData: String, delimitedBy: String, modelFieldDelim: String,
                          isAdhoc: Boolean = false): mutable.LinkedHashMap[String, String] = synchronized {
    val layout = new mutable.LinkedHashMap[String, String]
    if (segmentData == EMPTYSTR) return layout
    if (!isAdhoc) {
      // Storing Which Segment for Every Record makes Redundant data. Which already exist in Partition
      // layout += commonSegkey -> whichSeg
      layout += timeStampKey -> EMPTYSTR
      layout += fieldSeqNum -> EMPTYSTR
      commonNode.clone() transform ((k, v) => if (v ne EMPTYSTR) EMPTYSTR else v) foreach (ele => layout += ele)
    }
    (segmentData split(delimitedBy, -1)) foreach (ele => layout += ele -> EMPTYSTR)
    layout
  }
}

class EnrichCacheManager extends Logg {

  import com.hca.cdm.hl7.model.FieldsTransformer

  private lazy val cacheStore = new mutable.HashMap[String, FieldsTransformer]()

  def cache(unit: String, enricher: FieldsTransformer): Unit = cacheStore += unit -> enricher

  def getEnRicher(unit: String): Option[FieldsTransformer] = cacheStore.get(unit)

  def getCacheSer: Array[Byte] = serialize(cacheStore)


}

object EnrichCacheManager extends Logg {

  import com.hca.cdm.hl7.model.FieldsTransformer

  private var instance: EnrichCacheManager = _
  private val lock = new Object()

  def apply(): EnrichCacheManager = lock.synchronized {
    if (instance == null) instance = new EnrichCacheManager()
    instance
  }

  def apply(cacheSer: Array[Byte]): EnrichCacheManager = lock.synchronized {
    if (instance == null) {
      instance = new EnrichCacheManager()
      deSerialize[mutable.HashMap[String, FieldsTransformer]](cacheSer).foreach(x => instance.cache(x._1, x._2))
    }
    instance
  }

  def apply(adhocConfig: String, hl7Types: Array[String], offHeapHandler: ((Any,Any,Any,Any)) => Any): EnrichCacheManager = lock.synchronized {
    if (instance == null) {
      instance = new EnrichCacheManager()
      val adhocSeg = model.applySegmentsToAll(model.loadSegments(adhocConfig), hl7Types)
      hl7Types.foreach { hl7 => {
        model.segmentsForHl7Type(HL7Types.withName(hl7), adhocSeg(hl7))
          .models.foreach(_._2.foreach { model =>
          if (model.adhoc.isDefined) {
            instance.getEnRicher(model.adhoc.get.transformer).foreach(_.offHeapDataEnricher.init(offHeapHandler))
          }
        })

      }
      }

    }
    instance
  }

}

