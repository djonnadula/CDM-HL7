package com.cdm.hl7.parser.tests

import com.cdm.hl7.constants.HL7Types
import com.cdm.log.Logg
import org.scalatest.FlatSpec

/**
  * Created by Peter James on 3/1/2017.
  *
  * SIU Message Parsing Unit Tests
  */
class HL7ParserSIUTest extends FlatSpec with Logg {

    val messageType = HL7Types.SIU
    val hl7TestSetup = new HL7ParserTestSetup(messageType)
    val testFileBasePath = "/src/test/scala/com/hca/cdm/hl7/parser/tests/"

    val messageName1 = "SIU_1"
    val msg1 = HL7ParserTestUtils.getMessage(testFileBasePath, messageName1, messageType)
    val res1 = hl7TestSetup.parse(msg1)
    val expected1 = HL7ParserTestUtils.getExpected(testFileBasePath, messageName1, messageType)
    "SIU Message Test 1 (SIU_1)" should "have a match for the parsed output" in {
        assert(res1 === expected1)
    }

    // MT.2.4 Test
    val messageName2 = "SIU_2"
    val msg2 = HL7ParserTestUtils.getMessage(testFileBasePath, messageName2, messageType)
    val res2 = hl7TestSetup.parse(msg2)
    val expected2 = HL7ParserTestUtils.getExpected(testFileBasePath, messageName2, messageType)
    "SIU Message Test 2 (SIU_2)" should "have a match for the parsed output" in {
        assert(res2 === expected2)
    }

    val messageName3 = "SIU_3"
    val msg3 = HL7ParserTestUtils.getMessage(testFileBasePath, messageName3, messageType)
    val res3 = hl7TestSetup.parse(msg3)
    val expected3 = HL7ParserTestUtils.getExpected(testFileBasePath, messageName3, messageType)
    "SIU Message Test 3 (SIU_3)" should "have a match for the parsed output" in {
        assert(res3 === expected3)
    }

    // IP FRNK 2.5 Facility Overrides Test
    val messageName4 = "SIU_4"
    val msg4 = HL7ParserTestUtils.getMessage(testFileBasePath, messageName4, messageType)
    val res4 = hl7TestSetup.parse(msg4)
    val expected4 = HL7ParserTestUtils.getExpected(testFileBasePath, messageName4, messageType)
    "SIU Message Test 4 (SIU_4)" should "have a match for the parsed output" in {
        assert(res4 === expected4)
    }

    val messageName5 = "SIU_5"
    val msg5 = HL7ParserTestUtils.getMessage(testFileBasePath, messageName5, messageType)
    val res5 = hl7TestSetup.parse(msg5)
    val expected5 = HL7ParserTestUtils.getExpected(testFileBasePath, messageName5, messageType)
    "SIU Message Test 5 (SIU_5)" should "have a match for the parsed output" in {
        assert(res5 === expected5)
    }

    // MT.2.5 Test
    val messageName6 = "SIU_6"
    val msg6 = HL7ParserTestUtils.getMessage(testFileBasePath, messageName6, messageType)
    val res6 = hl7TestSetup.parse(msg6)
    val expected6 = HL7ParserTestUtils.getExpected(testFileBasePath, messageName6, messageType)
    "SIU Message Test 6 (SIU_6)" should "have a match for the parsed output" in {
        assert(res6 === expected6)
    }

    // MT.2.4 Test
    val messageName7 = "SIU_7"
    val msg7 = HL7ParserTestUtils.getMessage(testFileBasePath, messageName7, messageType)
    val res7 = hl7TestSetup.parse(msg7)
    val expected7 = HL7ParserTestUtils.getExpected(testFileBasePath, messageName7, messageType)
    "SIU Message Test 7 (SIU_7)" should "have a match for the parsed output" in {
        assert(res7 === expected7)
    }
}
