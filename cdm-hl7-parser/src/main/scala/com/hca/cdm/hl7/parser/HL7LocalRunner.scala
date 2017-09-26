package com.hca.cdm.hl7.parser

import com.hca.cdm.Models.MSGMeta
import com.hca.cdm._
import com.hca.cdm.hl7.model._
import com.hca.cdm.hl7.audit.AuditConstants._
import com.hca.cdm.hl7.audit._
import com.hca.cdm.hl7.constants.HL7Constants._
import com.hca.cdm.hl7.constants.HL7Types.{withName => hl7}
import com.hca.cdm.log.Logg
import org.apache.log4j.PropertyConfigurator._

import scala.util.{Failure, Success, Try}
import com.hca.cdm.mq.publisher.{MQAcker => TLMAcknowledger}
/**
  * Created by Devaraj Jonnadula on 8/24/2016.
  */
object HL7LocalRunner extends App with Logg {

  configure(currThread.getContextClassLoader.getResource("local-log4j.properties"))
  reload(null,Some(currThread.getContextClassLoader.getResourceAsStream("Hl7LocalConfig.properties")))
  val msgType = hl7("ORU")
    // hl7(args(0))
  private val msgs =
      "MSH|^~\\&|IMG_RESULT|MSLP||IMG_RESULT|20170630110925|EDIRADIN|ORU^R01|EPIC_MSLP_ORU_3669490|P|2.1||Epic_Mountain_PRD|||||||\nPID|1|W173439^^SLCURN^SLCURN|1000661485^EPIC^MRN|D000010054^MCC^MCC~W000266661^SMHP^SnMHP|MCKENNEY-DAVIES^SUSANNA^CLARA||19670522|F|BLUMER^SUSANNA^C~MCKENNEYDAVIES^SUSANNA^CLARA^~MCKENNEY DAVIES^SUSANNA^|W|366 N 1200 EPLEASANT GROVE^UT^84062^USA^P^UTAH|UTAH|(801)859-4955^P^H|(801)268-7119^P^W||M||100331415|074-62-9371|||||||||||\nPD1|||LONE PEAK HOSPITAL^10002001|BENRI^BENNETT^RICHARD^W^^^PROVID^^PROVID~MAQ9147^BENNETT^RICHARD^W^^^THREEFOUR^^^THREEFOUR||||||||||||\nPV1||O|MSLPRD^^MSLP^^^^^||||BENRI^BENNETT^RICHARD^W^^^PROVID^^PROVID~MAQ9147^BENNETT^RICHARD^W^^^THREEFOUR^^THREEFOUR|BENRI^BENNETT^RICHARD^W^^^PROVID^^PROVID~MAQ9147^BENNETT^RICHARD^W^^^THREEFOUR^^^THREEFOUR|||||||||||30227288|||||||||||||||||||||||||20170630104430|||||||V\nORC|RE|796251^EPC|MSLP849741^EPC||Final||^^20170630110055^20170630110115^R||20170630110925|EDIRADIN^INTERFACE^RAD^RESULTS IN||BENRI^BENNETT^RICHARD^W^^^^PROVID^^PROVID~MAQ9147^BENNETT^RICHARD^W^^^THREEFOUR^^THREEFOUR|10002001006^10002001^^^^UT LP XR|(801)572-0311|||||||||||||||I\nOBR|1|796251^EPC|MSLP849741^EPC|IMG115^XR HIP 2-3V LEFT^RAD^XR HIP 2+ VW|R|20170630104431|||||Ancillary Pe|||||BENRI^BENNETT^RICHARD^W^^^PROVID^^PROVID~MAQ9147^BENNETT^RICHARD^W^^^THREEFOUR^^THREEFOUR|(801)572-0311|||||20170630110800||RG|Final||^20170630110055^20170630110115^R||||^Annual Physical exam|PANCO^PANUSHKA^COLE^J^^^PROVID^^PROVID~MMI9374^PANUSHKA^COLE^J^^^THREEFOUR^^THREEFOUR||BBI7712^JONES^CHELSEA||20170630104500||||||||IMG115^XR HIP 2-3V LEFT^RADXR HIP 2+ VW|LT(used to identify procedures performed on the left side of\nOBX|1|ST|&GDT|1|||||||Final||||9|BBI7712^JONES^CHELSEA^^||\nOBX|2|ST|&GDT|1|XR HIP 2-3V LEFT||||||Final||||9|BBI7712^JONES^CHELSEA^^||\nOBX|3|ST|&GDT|1|||||||Final||||9|BBI7712^JONES^CHELSEA^^||\nOBX|4|ST|&GDT|1|HISTORY: 50 years old Female with history of left hip pain||||||Final||||9|BBI7712^JONES^CHELSEA^^||\nOBX|5|ST|&GDT|1|||||||Final||||9|BBI7712^JONES^CHELSEA^^||\nOBX|6|ST|&GDT|1|COMPARISON: 10/15/2008||||||Final||||9|BBI7712^JONES^CHELSEA^^||\nOBX|7|ST|&GDT|1|||||||Final||||9|BBI7712^JONES^CHELSEA^^||\nOBX|8|ST|&GDT|1|FINDINGS: ||||||Final||||9|BBI7712^JONES^CHELSEA^^||\nOBX|9|ST|&GDT|1|||||||Final||||9|BBI7712^JONES^CHELSEA^^||\nOBX|10|ST|&GDT|1|AP pelvis with abducted view of the left hip. Joints appear symmetric. Bony pelvis appears normal. There is no fracture dislocation. The articulating surfaces are smooth and normal contour.||||||Final||||9|BBI7712^JONES^CHELSEA^^||\nOBX|11|ST|&GDT|1|||||||Final||||9|BBI7712^JONES^CHELSEA^^||\nOBX|12|ST|&IMP|2|||||||Final||||9|BBI7712^JONES^CHELSEA^^||\nOBX|13|ST|&IMP|2|IMPRESSION:||||||Final||||9|BBI7712^JONES^CHELSEA^^||\nOBX|14|ST|&IMP|2|||||||Final||||9|BBI7712^JONES^CHELSEA^^||\nOBX|15|ST|&IMP|2|||||||Final||||9|BBI7712^JONES^CHELSEA^^||\nOBX|16|ST|&IMP|2|Normal appearance the pelvis and left hip. No fracture dislocation.||||||Final||||9|BBI7712^JONES^CHELSEA^^||\nOBX|17|ST|&IMP|2|||||||Final||||9|BBI7712^JONES^CHELSEA^^||\nOBX|18|ST|&IMP|2|** Electronically Signed by Cole Panushka, MD on 6/30/2017 11:08 AM **||||||Final||||9|BBI7712^JONES^CHELSEA^^||\nOBX|19|ST|&GDT|2|||||||Final||||9|BBI7712^JONES^CHELSEA^^||\nOBX|20|ST|&GDT|2|Electronically Signed By Cole J Panushka, MD on 6/30/2017 11:08||||||Final||||9|BBI7712^JONES^CHELSEA^^||\nOBX|21|ST|&GDT|2|||||||Final||||9|BBI7712^JONES^CHELSEA^^||\nOBX|22|ST|&TCM|3|Px in left hip; trauma injury few years ago; kni injury since then; px comes and goes for last few years||||||Final||||9|BBI7712^JONES^CHELSEA^^||\nZDS|1.2.840.114350.2.340.2.798268.2.796251.1^EPIC^APPLICATION^DICOM"
      //"MSH|^~\\&|COCNW|COCNW|OV|OV|201706092307||RAS^O17|IP_COCNW_RAS_PHAWF.170609.2875|P|2.5||CAC^01554||\nPID|1||AF00769225^^^COCWF^MRN|AF713617|ELHAGE^KATIA||19540601|F||||||||||AF1002484898\nPV1|1|I|AF.5W^AF.502^1^COCWF|EM|||SHARAM^Shaarawy^Rami|||||||||||IN|||||||||||||||||||||||ADM\nORC|RE|14494418|08438036||AC||^ASDIR^As Directed||201706092306|||SHARAM|||201706091855\nRXA|1|1|201706092306||POT20/100^POTASSIUM CHLORIDE 20 MEQ/100 ML STER H20 PREMIX BAG^L^00338070548A^POTASSIUM CHLORIDE 20MEQ^NDC|1|MLS|100^1|026348|AFNURIXA|||||||||Y^Y|CP|D|||||IV\nRXR|IV^INTRAVEN.|VAD\nNTE|1|CO|Difference between amount dispensed\nNTE|2|CO|and amount administered was discarded.\nZSC|1|POT20/100^0338070548^POTASSIUM CHLORIDE 20 MEQ/100 ML STER H20 PREMIX BAG^POTASSIUM CHLORIDE 20MEQ^POTASSIUM CHLORIDE\n"
     // "MSH|^~\\&||COCMO|||201706112324||ADT^A02|MT_COCMO_ADT_MOGTADM.1.15851416|P|2.1\nEVN|A02|201706112324|||GNUR.ACM3^MAGALLANEZ^ADELL^^^^\nPID|1||G000326406|G227729|WILSON^TERRY^LYNN^^^|MARIAM|19560509|F|^^^^^|W|3315 INDIAN HORSE CT^^NORTH LAS VEGAS^NV^89032^USA^^^CLARK||(702)767-8574|(702)365-7111|ENG|M|OTH|G00016436394|561-98-5748\nPV1|1|I|G.2SE^G.2019^A|EM||G.ERH^G.ED10^A|QUIDU^Quiroz^Dulce^^^^DO|.SELF^REFERRED^SELF^^^^|.NO PCP^PHYSICIAN^NO^PRIMARY OR FAMILY^^^|MED||||PR|FR|N|QUIDU^Quiroz^Dulce^^^^DO|IN||08|||||||||||||||||||COCMO|GENERALIZED WEAKNESS, HYPERGLYCEMIA, UTI|ADM|||201706112203\nAL1|1|DA|F001000815^No Known Drug Intolerances^^^allergy.id|U|NKDA|20130423\nAL1|2|DA|No Known Contrast Allergies|U||20061211\nAL1|3|DA|No Known Drug Allergies|U||20061211\nAL1|4|DA|No Known Food Allergies|U||20061211\nAL1|5|DA|No Known Other Allergies|U||20061211\nACC|20170611^|11\nDG1|1|I10|R53.1|WEAKNESS||A||||||||||||N\nGT1|1||WILSON^TERRY^LYNN^^^||3315 INDIAN HORSE CT^^NORTH LAS VEGAS^NV^89032^USA^^^CLARK|(702)767-8574||19560509|F||SA|561-98-5748|||ORLEANS HOTEL   CASINO|4500 W TROPICANA AVE.^^LAS VEGAS^NV^89103|(702)365-7111|||F\nGT1|2||^^^^^||^^^^^^^^|||||||||||^^^^\nIN1|1|BCOOS||BLUE CROSS OUT OF STATE|PO BOX 5747^^DENVER^CO^80217^USA||(877)833-5742|174172M2AS|BOYD GAMING|||20170101||||WILSON^TERRY^L^^^|01|19560509||||||||||||||||||BYM963M78509|||||||F\nZMR|1|H000059103\nZCD|1|1REG0M031A^4. Illness/injury due to work related accident/condition?^N\nZCD|2|1REG0M041A^1. Was illness/injury due to a non-work related accident?^NO\nZCD|3|1REG0M065A^Occurrence Code:^11\nZCD|4|1REG0M066A^Date:^20170611\nZCD|5|1REG0M234A^FC1:^08\nZCD|6|ETHNICITY^ETHNICITY^2\nZCD|7|ZSS.AMTREQ^AMT REQ^200.00\nZCD|8|ZSS.DEPREQ^DEPOSIT REQ?^N\nZCD|9|ZSS.ESTCHG^EST PT DUE^0.00\nZCD|10|ZSS.UNABLE^Comment if FULL amt requested NOT collected^NOT INF\nZIN|1|SP|BLUE CROSS|Y|20170611|PPI||Y|||||BCOOS\nZCS|ORLHO|4500 W TROPICANA AVE.^^LAS VEGAS^NV^89103|F|DECLINED|DECLINED|02270\n"
    //  "MSH|^~\\&|MT_COCQA1A|COCQA1A|DBM||201601190838||ADT^A02|MT_COCQA1A_ADT_QA1AGTADM.1.229576.567|D|2.5||KYA\nEVN|A02|201601190838|||1TSQBE8554^HAMMOCK^BRITTANY^^^^\nPID|1||J000423598|J500217|Jones^Jessica^Campbell^^|UNKNOWN|20031230|F|CAGE^JESSICA^CAMPBELL JONES^|U|456 MARVEL STREETFRANKLIN^TN^37064^USA^^WILLIAMSON||(615)555-6261|(615)555-6261|ENG|S|NON|J00021004053|302-30-2015\nPV1|1|I|J.DEVICU^J.DEVICU1^A|EL||J.DE2^J.DE2^229|DRFIRST^FIRST^DOCTOR^^^||DRBRI^DOCTOR^BRITTANY^^|IM||||ER||Y|ADMITTING^PHYSICIAN^TEST^ADMITTING^^|INO||08|||||||||||||||||||COCQA1A|OBSERVATION|ADM|||201601190811\nAL1|1|DA|1000031^Bee^Bee|SV|ANAPHYLAXIS|20160114\nAL1|2|DA|F006004630^sufentanil^sufentanil|MI|ANXIETY|20160114\nACC|20160119^|11\nGT1|1||Jones^Jessica^Campbell^^||456 MARVEL STREETFRANKLIN^TN^37064^USA^WILLIAMSON|(615)555-6261||20031230|F||SA|302-30-2015|||ALIAS|^^^||||FT\nGT1|2||^^^^||^^^^^^|||||||||||^^^\nIN1|1|AETNA||AETNA|10 WHITE STREET^INS ST2^ANYTOWN^TN^00111^USA||(615)825-5555|AETNA GROUP #|AETNA GROUP NAME|||||||Jones^Jessica^Campbell^^^|01|20031230||||||||||||||||||302302015|||||||F\nZCD|1|ETHNICITY^ETHNICITY^3\nZCD|2|Z.ADDLRACE^ADDITIONAL RACE^U\nZCD|3|ZSS.DEPREQ^DEPOSIT REQ?^N\nZCD|4|ZSS.UNABLE^Comment if FULL amt requested NOT collected^NR\nZCD|5|hca0000200^Reason-^2\nZCD|6|hca0000227^Able to perform TB Contagious Respiratory Infection Point of Entry Screen^N\nZIN|1|SP|AETNA INSURANCE|N||AET PRECERT CONTACT||N|||||AETNA\nZCS|ALIAS|456 MARVEL STREET^^FRANKLIN^TN^37064|FT|EMAIL@TEST.COM|EMAIL@TEST.COM|04446"
    //"MSH|^~\\&||COCHM|||201703310757||ADT^A31|MT_COCHM_ADT_TNAGTMRI.1.7325521|P|2.1\nEVN|A31|201703310757||E\nPID|1||Q002023752|Q95587|GUPTON^BRANDY^LEIGH^^^|ANGELA|19900517|F|GUPTON^BRANDY^^^^|W|2704 HWY 47 NORTH^^WHITE BLUFF^TN^37187^USA^^^DICKSO.TN||615-972-6793|615-446-8000|ENG|S|NON|Q95587|412-65-5356\nIN1|1|TC.AMERIC||TNCARE AMERICHOICE|PO BOX 5220^STE 200^KINGSTON^NY^12402-5220||800-690-1606|MT01|TNCARE AMERICHOICE|||20081001|20110516|||GUPTON^BRANDY^L^^^|01|||||||||||||||||||JD3794405\nIN1|2|TC.AMGRP||TNCARE AMERIGROUP|PO BOX 61010^TN CLAIMS^VIRGINIA BEACH^VA^23466-1010||800-454-3730|||||||||GUPTON^BRANDY^L^^^|01|||||||||||||||||||712927939\nIN1|3|TC.BC||TNCARE BLUECARE|1 CAMERON HILL CIRCLE^SUITE 0002^CHATTANOOGA^TN^37402-0002||800-276-1978|125000||||20150101||||GUPTON^BRANDY^^^^|01|||||||||||||||||||ZECM12714949\nIN1|4|TC.UHCCMPL||TNCARE UHC COMMUNITY PLAN|PO BOX 5220^^KINGSTON^NY^12402-5220||800-690-1606|MT05||||20110601||||GUPTON^BRANDY^L^^^|01|||||||||||||||||||JD3794405\nIN1|5|UNINSURED||UNINSURED DISCOUNT PLAN|552 METROPLEX DR^^NASHVILLE^TN^37211||615-886-5660|||||||||GUPTON^BRANDY^LEIGH^^^|01|||||||||||||||||||412655356\n"
    //"MSH|^~\\&||COCXG|||201701231800||RDE^O01|MT_COCXG_RDE_XGPHAORD.1.10910266|P|2.2\nPID|||J000466169|J429090|KERR^BURGRTT^ULANE^^^||19499517|||||||||||J00073669669\nPV1|I||J.PACUH^J.801^A||||LAZJE^Lazarus^Jeffrey^J MD"

  private val messageTypes = lookUpProp("hl7.messages.type") split COMMA
  private val hl7MsgMeta = messageTypes.map(mtyp => hl7(mtyp) -> getMsgTypeMeta(hl7(mtyp), lookUpProp(mtyp + ".kafka.source"))) toMap
  private val templatesMapping = loadTemplate(lookUpProp("hl7.template"))
  private val segmentsMapping = applySegmentsToAll(loadSegments(lookUpProp("hl7.segments")) ++ loadSegments(lookUpProp("hl7.adhoc-segments")), messageTypes)
  private val modelsForHl7 = hl7MsgMeta.map(msgType => msgType._1 -> segmentsForHl7Type(msgType._1, segmentsMapping(msgType._1.toString)))
  private val registeredSegmentsForHl7 = modelsForHl7.mapValues(_.models.keySet)
  private val hl7Parsers = hl7MsgMeta map (hl7 => hl7._1 -> new HL7Parser(hl7._1, templatesMapping))
  private val jsonAuditor = hl7MsgMeta map (msgType => msgType._1 -> (auditMsg(msgType._1.toString, jsonStage)(EMPTYSTR, _: MSGMeta)))
  private val segmentsAuditor = hl7MsgMeta map (msgType => msgType._1 -> (auditMsg(msgType._1.toString, segmentStage)(_: String, _: MSGMeta)))
  private val adhocAuditor = hl7MsgMeta map (msgType => msgType._1 -> (auditMsg(msgType._1.toString, adhocStage)(_: String, _: MSGMeta)))
  private val allSegmentsInHl7Auditor = hl7MsgMeta map (msgType => msgType._1 -> (auditMsg(msgType._1.toString, segmentsInHL7)(_: String, _: MSGMeta)))
  private val tlmAuditor = tlmAckMsg("test", applicationReceiving, HDFS, _: String)(_: MSGMeta)
  private val segmentsHandler = modelsForHl7 map (hl7 => hl7._1 -> new DataModelHandler(hl7._2, registeredSegmentsForHl7(hl7._1), segmentsAuditor(hl7._1),
    allSegmentsInHl7Auditor(hl7._1), adhocAuditor(hl7._1), tlmAckMsg(hl7._1.toString, applicationReceiving, HDFS, _: String)(_: MSGMeta)))
  var tlmAckIO: (String, String) => Unit = null
  modelsForHl7.filter(_._1.toString == "MDM")
    //.filter(_._2.models.filter(_._1.contains("ADHOC")))
    TLMAcknowledger("test", "test")(lookUpProp("mq.hosts"), lookUpProp("mq.manager"), lookUpProp("mq.channel"), lookUpProp("mq.destination.queues"))
    tlmAckIO = TLMAcknowledger.ackMessage(_: String, _: String)

  Try(hl7Parsers(msgType).transformHL7(msgs, reject) rec) match {
    case Success(map) =>
      map match {
        case Left(out) =>
          info("json: " + out._1)
          segmentsHandler(msgType).handleSegments(outio, templateerrorReject, audit, adhocDestination,Some(tlmAckIO))(out._2, msgs, out._3)
        case Right(t) =>
          error(t);
      }
    case Failure(t) =>
      error(t)
  }

  private def outio(k: String, v: String) = {
   // info("outio: " + k)
  }

  private def reject(k: AnyRef, v: String) = {
    info("reject: " + k)
  }
  private def templateerrorReject(k: AnyRef, v: String) = {
    info("reject: " + k+ " " + v)
  }
  private def audit(k: String, v: String) = {
   // info("audit: " + k)
  }

  private def adhocDestination(k: String, v: String, dest: String) = {
    info("adhocDestiation: " + k)
  }

}





