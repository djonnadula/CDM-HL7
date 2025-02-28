package com.cdm.hl7.parser.tests

import com.cdm.hl7.constants.HL7Types
import com.cdm.log.Logg
import org.scalatest.FlatSpec

/**
  * Created by Peter James on 3/1/2017.
  *
  * PPR Message Parsing Unit Tests
  */
class HL7ParserPPRTest extends FlatSpec with Logg {

    val messageType = HL7Types.PPR
    val hl7TestSetup = new HL7ParserTestSetup(messageType)
    val testFileBasePath = "/src/test/scala/com/hca/cdm/hl7/parser/tests/"

    val messageName1 = "PPR_1"
    val msg1 = HL7ParserTestUtils.getMessage(testFileBasePath, messageName1, messageType)
    val res1 = hl7TestSetup.parse(msg1)
    val expected1 = HL7ParserTestUtils.getExpected(testFileBasePath, messageName1, messageType)
    "PPR Message Test 1 (PPR_1)" should "have a match for the parsed output" in {
        assert(res1 === expected1)
    }

    val messageName2 = "PPR_2"
    val msg2 = HL7ParserTestUtils.getMessage(testFileBasePath, messageName2, messageType)
    val res2 = hl7TestSetup.parse(msg2)
    val expected2 = HL7ParserTestUtils.getExpected(testFileBasePath, messageName2, messageType)
    "PPR Message Test 2 (PPR_2)" should "have a match for the parsed output" in {
        assert(res2 === expected2)
    }

    val messageName3 = "PPR_3"
    val msg3 = HL7ParserTestUtils.getMessage(testFileBasePath, messageName3, messageType)
    val res3 = hl7TestSetup.parse(msg3)
    val expected3 = HL7ParserTestUtils.getExpected(testFileBasePath, messageName3, messageType)
    "PPR Message Test 3 (PPR_3)" should "have a match for the parsed output" in {
        assert(res3 === expected3)
    }
}
