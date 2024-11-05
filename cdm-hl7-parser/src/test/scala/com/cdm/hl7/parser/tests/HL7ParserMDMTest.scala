package com.cdm.hl7.parser.tests

import com.cdm.hl7.constants.HL7Types
import com.cdm.log.Logg
import org.scalatest.FlatSpec

/**
  *
  * MDM Message Parsing Unit Tests
  */
class HL7ParserMDMTest extends FlatSpec with Logg {

    val messageType = HL7Types.MDM
    val hl7TestSetup = new HL7ParserTestSetup(messageType)
    val testFileBasePath = "/src/test/scala/com//cdm/hl7/parser/tests/"

    val messageName1 = "MDM_1"
    val msg1 = HL7ParserTestUtils.getMessage(testFileBasePath, messageName1, messageType)
    val res1 = hl7TestSetup.parse(msg1)
    val expected1 = HL7ParserTestUtils.getExpected(testFileBasePath, messageName1, messageType)
    "MDM Message Test 1 (MDM_1)" should "have a match for the parsed output" in {
        assert(res1 === expected1)
    }

    val messageName2 = "MDM_2"
    val msg2 =  HL7ParserTestUtils.getMessage(testFileBasePath, messageName2, messageType)
    val res2 = hl7TestSetup.parse(msg2)
    val expected2 =  HL7ParserTestUtils.getExpected(testFileBasePath, messageName2, messageType)
    "MDM Message Test 2 (MDM_2)" should "have a match for the parsed output" in {
        assert(res2 === expected2)
    }

    val messageName3 = "MDM_3"
    val msg3 = HL7ParserTestUtils.getMessage(testFileBasePath, messageName3, messageType)
    val res3 = hl7TestSetup.parse(msg3)
    val expected3 = HL7ParserTestUtils.getExpected(testFileBasePath, messageName3, messageType)
    "MDM Message Test 3 (MDM_3)" should "have a match for the parsed output" in {
        assert(res3 === expected3)
    }

    val messageName4 = "MDM_4"
    val msg4 = HL7ParserTestUtils.getMessage(testFileBasePath, messageName4, messageType)
    val res4 = hl7TestSetup.parse(msg4)
    val expected4 = HL7ParserTestUtils.getExpected(testFileBasePath, messageName4, messageType)
    "MDM Message Test 4 (MDM_4)" should "have a match for the parsed output" in {
        assert(res4 === expected4)
    }

    val messageName5 = "MDM_5"
    val msg5 = HL7ParserTestUtils.getMessage(testFileBasePath, messageName5, messageType)
    val res5 = hl7TestSetup.parse(msg5)
    val expected5 = HL7ParserTestUtils.getExpected(testFileBasePath, messageName5, messageType)
    "MDM Message Test 5 (MDM_5)" should "have a match for the parsed output" in {
        assert(res5 === expected5)
    }

    val messageName6 = "MDM_6"
    val msg6 = HL7ParserTestUtils.getMessage(testFileBasePath, messageName6, messageType)
    val res6 = hl7TestSetup.parse(msg6)
    val expected6 = HL7ParserTestUtils.getExpected(testFileBasePath, messageName6, messageType)
    "MDM Message Test 6 (MDM_6)" should "have a match for the parsed output" in {
        assert(res6 === expected6)
    }

    val messageName7 = "MDM_7"
    val msg7 = HL7ParserTestUtils.getMessage(testFileBasePath, messageName7, messageType)
    val res7 = hl7TestSetup.parse(msg7)
    val expected7 = HL7ParserTestUtils.getExpected(testFileBasePath, messageName7, messageType)
    "MDM Message Test 7 (MDM_7)" should "have a match for the parsed output" in {
        assert(res7 === expected7)
    }

    val messageName8 = "MDM_8"
    val msg8 = HL7ParserTestUtils.getMessage(testFileBasePath, messageName8, messageType)
    val res8 = hl7TestSetup.parse(msg8)
    val expected8 = HL7ParserTestUtils.getExpected(testFileBasePath, messageName8, messageType)
    "MDM Message Test 8 (MDM_8)" should "have a match for the parsed output" in {
        assert(res8 === expected8)
    }

    val messageName9 = "MDM_9"
    val msg9 = HL7ParserTestUtils.getMessage(testFileBasePath, messageName9, messageType)
    val res9 = hl7TestSetup.parse(msg9)
    val expected9 = HL7ParserTestUtils.getExpected(testFileBasePath, messageName9, messageType)
    "MDM Message Test 9 (MDM_9)" should "have a match for the parsed output" in {
        assert(res9 === expected9)
    }

    val messageName10 = "MDM_10"
    val msg10 = HL7ParserTestUtils.getMessage(testFileBasePath, messageName10, messageType)
    val res10 = hl7TestSetup.parse(msg10)
    val expected10 = HL7ParserTestUtils.getExpected(testFileBasePath, messageName10, messageType)
    "MDM Message Test 10 (MDM_10)" should "have a match for the parsed output" in {
        assert(res10 === expected10)
    }

    val messageName11 = "MDM_11"
    val msg11 = HL7ParserTestUtils.getMessage(testFileBasePath, messageName11, messageType)
    val res11 = hl7TestSetup.parse(msg11)
    val expected11 = HL7ParserTestUtils.getExpected(testFileBasePath, messageName11, messageType)
    "MDM Message Test 11 (MDM_11)" should "have a match for the parsed output" in {
        assert(res11 === expected11)
    }

    val messageName12 = "MDM_12"
    val msg12 = HL7ParserTestUtils.getMessage(testFileBasePath, messageName12, messageType)
    val res12 = hl7TestSetup.parse(msg12)
    val expected12 = HL7ParserTestUtils.getExpected(testFileBasePath, messageName12, messageType)
    "MDM Message Test 12 (MDM_12)" should "have a match for the parsed output" in {
        assert(res12 === expected12)
    }

    val messageName13 = "MDM_13"
    val msg13 = HL7ParserTestUtils.getMessage(testFileBasePath, messageName13, messageType)
    val res13 = hl7TestSetup.parse(msg13)
    val expected13 = HL7ParserTestUtils.getExpected(testFileBasePath, messageName13, messageType)
    "MDM Message Test 13 (MDM_13)" should "have a match for the parsed output" in {
        assert(res13 === expected13)
    }

    val messageName14 = "MDM_14"
    val msg14 = HL7ParserTestUtils.getMessage(testFileBasePath, messageName14, messageType)
    val res14 = hl7TestSetup.parse(msg14)
    val expected14 = HL7ParserTestUtils.getExpected(testFileBasePath, messageName14, messageType)
    "MDM Message Test 14 (MDM_14)" should "have a match for the parsed output" in {
        assert(res14 === expected14)
    }
}
