package com.cdm.job.psg.aco.tests

import com.cdm.hl7.constants.HL7Constants._
import com.cdm.job.psg.aco.PsgAcoAdtJobUtils._
import com.cdm.log.Logg
import org.scalatest.FlatSpec

/**
  * Created by dof7475 on 7/20/2017.
  */
class PSGACOADTTest extends FlatSpec with Logg {

  // splitAndReturn
  val seg = "MSH|^~\\&||COCBR|"
  val delimiter = "\\|"
  val posIndex = 3
  val splitReturnPos = splitAndReturn(seg, delimiter, posIndex)
  "splitAndReturn (positive test)" should "be successful" in {
    assert(splitReturnPos.isSuccess)
  }
  it should s"return the index:$posIndex item in the segment" in {
    assert(splitReturnPos.get == "COCBR")
  }

  val negIndex = 100
  val splitReturnNeg = splitAndReturn(seg, delimiter, negIndex)
  "splitAndReturn (negative test)" should "fail" in {
    assert(splitReturnNeg.isFailure)
  }

  // trySplit
  val trySplitPos = trySplit(seg, delimiter)
  s"trySplit (positive test)" should "be successful" in {
    assert(trySplitPos.isSuccess)
  }
  it should "return a Try[Array[String]]" in {
    assert(trySplitPos.get sameElements Array[String]("MSH", "^~\\&", "", "COCBR"))
  }

  // segment
  val segtest = Array[String]("MSH|^~\\&||COCLW|||201707201534||ADT^A04|MT_COCLW_ADT_LWGTADM.1.1|P|2.1\n",
    "EVN|A04|201707201534|||TEST123^TEST^TEST^^^^\n",
    "PID|1||TEST123456|T123456|TEST^TEST^^^^||19400819|F|TEST^TEST^^^^|W|100 TEST ST^^TEST^FL^34222^USA^^^MAN||(999)999-9999|.|ENG|D|BAP|TEST987654321|999-99-9999|||\n",
    "IN1|1|BLOPPC||Medicare Advantage|PO BOX 1798^^JACKSONVILLE^FL^32231-0014^||(999)999-9999|999999|HARBOR FREIGHT TOOLS|||20170101||||^^^^^|01|19990101||||||||||||||||||12345678|||||||M")

  val searchPos = "PID"
  val segPos = segment(segtest, searchPos)
  s"segment (positive test), search $searchPos" should "be nonEmpty" in {
    assert(segPos.nonEmpty)
  }
  it should s"return the $searchPos" in {
    assert(segPos.head == "PID|1||TEST123456|T123456|TEST^TEST^^^^||19400819|F|TEST^TEST^^^^|W|100 TEST ST^^TEST^FL^34222^USA^^^MAN||(999)999-9999|.|ENG|D|BAP|TEST987654321|999-99-9999|||\n")
  }

  val searchNeg = "GAH"
  val segNeg = segment(segtest, searchNeg)
  s"segment (negative test), search $searchNeg" should "be empty (searching for segment GAH)" in {
    assert(segNeg.isEmpty)
  }

  // eventTypeMatch
  val adtTypesPos = Array[String]("A01", "A02", "A03", "A04")
  val eventTypeMatchPos = eventTypeMatch(segment(segtest, "MSH"), adtTypesPos)
  "eventTypeMatch (positive test)" should "be true when the event type matches" in {
    assert(eventTypeMatchPos)
  }

  val adtTypesNeg = Array[String]("A22")
  val eventTypeMatchNeg = eventTypeMatch(segment(segtest, "MSH"), adtTypesNeg)
  "eventTypeMatch (negative test)" should "be false when the event type does not match" in {
    assert(!eventTypeMatchNeg)
  }

  // singleFieldMatch
  val facArrayPos = Array[AnyRef]("COCLW", "COCCN")
  val facilityMatchPos = singleFieldMatch(segment(segtest, "MSH"), facArrayPos, "\\|", 3)
  "facilityMatchPos (positive test)" should "be true when a facilty is matched" in {
    assert(facilityMatchPos)
  }

  val facArrayNeg = Array[AnyRef]("HERE", "THERE")
  val facilityMatchNeg = singleFieldMatch(segment(segtest, "MSH"), facArrayNeg, "\\|", 3)
  "facilityMatchPos (negative test)" should "be false when there is no facility match" in {
    assert(!facilityMatchNeg)
  }

  // removeField
  val removeSSNPos = removeField(segment(segtest, PID), "\\|", 19)
  "removeField (positive test)" should "remove the SSN from the PID segment" in {
    assert(removeSSNPos == "PID|1||TEST123456|T123456|TEST^TEST^^^^||19400819|F|TEST^TEST^^^^|W|100 TEST ST^^TEST^FL^34222^USA^^^MAN||(999)999-9999|.|ENG|D|BAP|TEST987654321||||\n")
  }

  val segtest2 = Array[String]("MSH|^~\\&||COCLW|||201707201534||ADT^A04|MT_COCLW_ADT_LWGTADM.1.1|P|2.1\n",
    "EVN|A04|201707201534|||TEST123^TEST^TEST^^^^\n",
    "PID|1||TEST123456|T123456|TEST^TEST^^^^||19400819|F|TEST^TEST^^^^|W|100 TEST ST^^TEST^FL^34222^USA^^^MAN||(999)999-9999|.|ENG|D|BAP|TEST987654321||||\n",
    "IN1|1|BLOPPC||BC OUT OF STATE PPC|PO BOX 1798^^JACKSONVILLE^FL^32231-0014^||(999)999-9999|999999|HARBOR FREIGHT TOOLS|||20170101||||^^^^^|01|19990101||||||||||||||||||12345678|||||||M")

  val removeSSNNeg = removeField(segment(segtest2, PID), "\\|", 19)
  "removeField (negative test)" should "return the same segment when no SSN is present" in {
    assert(removeSSNNeg == "PID|1||TEST123456|T123456|TEST^TEST^^^^||19400819|F|TEST^TEST^^^^|W|100 TEST ST^^TEST^FL^34222^USA^^^MAN||(999)999-9999|.|ENG|D|BAP|TEST987654321||||\n")
  }

  val segtest3 = Array[String]("MSH|^~\\&||COCLW|||201707201534||ADT^A04|MT_COCLW_ADT_LWGTADM.1.1|P|2.1\n",
    "EVN|A04|201707201534|||TEST123^TEST^TEST^^^^\n",
    "IN1|1|BLOPPC||BC OUT OF STATE PPC|PO BOX 1798^^JACKSONVILLE^FL^32231-0014^||(999)999-9999|999999|HARBOR FREIGHT TOOLS|||20170101||||^^^^^|01|19990101||||||||||||||||||12345678|||||||M")

  val removeSSNNeg2 = removeField(segment(segtest3, PID), "\\|", 19)
  it should "return empty string when there is no PID" in {
    assert(removeSSNNeg2 == "")
  }

  // stringMatcher
  val stringMatcherArrayPos = "Medicare".split(",")
  val stringMatcherPos = stringMatcher(segment(segtest, PRIMARY_IN1), stringMatcherArrayPos, "\\|", 4)
  "stringMatcher (positive test)" should "match on Medicare" in {
    assert(stringMatcherPos)
  }

  val stringMatcherArrayNeg = "Medicarically".split(",")
  val stringMatcherNeg = stringMatcher(segment(segtest, PRIMARY_IN1), stringMatcherArrayNeg, "\\|", 4)
  "stringMatcher (negative test)" should "not have a match (matching on Medicarically)" in {
    assert(!stringMatcherNeg)
  }

  // getField
  val getFieldPos = getField(segment(segtest, PID), "\\|", 19)
  "getField (positive test)" should "return the SSN from the PID segment" in {
    assert(getFieldPos == "999-99-9999")
  }

  val getFieldNeg = getField(segment(segtest, PID), "\\|", 2)
  "getField (negative test)" should "return an empty string when no value exists" in {
    assert(getFieldNeg == "")
  }
}
