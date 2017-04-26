package com.hca.cdm.hl7.parser.tests

import com.hca.cdm.hl7.constants.HL7Types
import org.scalatest.FlatSpec

/**
  * Created by Peter James on 3/1/2017.
  *
  * ORU Message Parsing Unit Tests
  */
class HL7ParserORUTest extends FlatSpec{

    val messageType = HL7Types.ORU
    val hl7TestSetup = new HL7ParserTestSetup(messageType)
    val testFileBasePath = "/src/test/scala/com/hca/cdm/hl7/parser/tests/"
    hl7TestSetup.loadProperties("Hl7TestConfig.properties")

    val messageName1 = "ORU_1"
    val msg1 = HL7ParserTestUtils.getMessage(testFileBasePath, messageName1, messageType)
    val res1 = hl7TestSetup.parse(msg1)
    val expected1 = HL7ParserTestUtils.getExpected(testFileBasePath, messageName1, messageType)
    "ORU Message Test 1 (ORU_1)" should "have a match for the parsed output" in {
        assert(res1 === expected1)
    }

    val messageName2 = "ORU_2"
    val msg2 = HL7ParserTestUtils.getMessage(testFileBasePath, messageName2, messageType)
    val res2 = hl7TestSetup.parse(msg2)
    val expected2 = HL7ParserTestUtils.getExpected(testFileBasePath, messageName2, messageType)
    "ORU Message Test 2 (ORU_2)" should "have a match for the parsed output" in {
        assert(res2 === expected2)
    }

    val messageName3 = "ORU_3"
    val msg3 = HL7ParserTestUtils.getMessage(testFileBasePath, messageName3, messageType)
    val res3 = hl7TestSetup.parse(msg3)
    val expected3 = HL7ParserTestUtils.getExpected(testFileBasePath, messageName3, messageType)
    "ORU Message Test 3 (ORU_3)" should "have a match for the parsed output" in {
        assert(res3 === expected3)
    }

    val messageName4 = "ORU_4"
    val msg4 = HL7ParserTestUtils.getMessage(testFileBasePath, messageName4, messageType)
    val res4 = hl7TestSetup.parse(msg4)
    val expected4 = HL7ParserTestUtils.getExpected(testFileBasePath, messageName4, messageType)
    "ORU Message Test 4 (ORU_4)" should "have a match for the parsed output" in {
        assert(res4 === expected4)
    }

    val messageName5 = "ORU_5"
    val msg5 = HL7ParserTestUtils.getMessage(testFileBasePath, messageName5, messageType)
    val res5 = hl7TestSetup.parse(msg5)
    val expected5 = HL7ParserTestUtils.getExpected(testFileBasePath, messageName5, messageType)
    "ORU Message Test 5 (ORU_5)" should "have a match for the parsed output" in {
        assert(res5 === expected5)
    }

    val messageName6 = "ORU_6"
    val msg6 = HL7ParserTestUtils.getMessage(testFileBasePath, messageName6, messageType)
    val res6 = hl7TestSetup.parse(msg6)
    val expected6 = HL7ParserTestUtils.getExpected(testFileBasePath, messageName6, messageType)
    "ORU Message Test 6 (ORU_6)" should "have a match for the parsed output" in {
        assert(res6 === expected6)
    }

    val messageName7 = "ORU_7"
    val msg7 = HL7ParserTestUtils.getMessage(testFileBasePath, messageName7, messageType)
    val res7 = hl7TestSetup.parse(msg7)
    val expected7 = HL7ParserTestUtils.getExpected(testFileBasePath, messageName7, messageType)
    "ORU Message Test 7 (ORU_7)" should "have a match for the parsed output" in {
        assert(res7 === expected7)
    }

    val messageName8 = "ORU_8"
    val msg8 = HL7ParserTestUtils.getMessage(testFileBasePath, messageName8, messageType)
    val res8 = hl7TestSetup.parse(msg8)
    val expected8 = HL7ParserTestUtils.getExpected(testFileBasePath, messageName8, messageType)
    "ORU Message Test 8 (ORU_8)" should "have a match for the parsed output" in {
        assert(res8 === expected8)
    }

    val messageName9 = "ORU_9"
    val msg9 = HL7ParserTestUtils.getMessage(testFileBasePath, messageName9, messageType)
    val res9 = hl7TestSetup.parse(msg9)
    val expected9 = HL7ParserTestUtils.getExpected(testFileBasePath, messageName9, messageType)
    "ORU Message Test 9 (ORU_9)" should "have a match for the parsed output" in {
        assert(res9 === expected9)
    }

    val messageName10 = "ORU_10"
    val msg10 = HL7ParserTestUtils.getMessage(testFileBasePath, messageName10, messageType)
    val res10 = hl7TestSetup.parse(msg10)
    val expected10 = HL7ParserTestUtils.getExpected(testFileBasePath, messageName10, messageType)
    "ORU Message Test 10 (ORU_10)" should "have a match for the parsed output" in {
        assert(res10 === expected10)
    }

    val messageName11 = "ORU_11"
    val msg11 = HL7ParserTestUtils.getMessage(testFileBasePath, messageName11, messageType)
    val res11 = hl7TestSetup.parse(msg11)
    val expected11 = HL7ParserTestUtils.getExpected(testFileBasePath, messageName11, messageType)
    "ORU Message Test 11 (ORU_11)" should "have a match for the parsed output" in {
        assert(res11 === expected11)
    }

    val messageName12 = "ORU_12"
    val msg12 = HL7ParserTestUtils.getMessage(testFileBasePath, messageName12, messageType)
    val res12 = hl7TestSetup.parse(msg12)
    val expected12 = HL7ParserTestUtils.getExpected(testFileBasePath, messageName12, messageType)
    "ORU Message Test 12 (ORU_12)" should "have a match for the parsed output" in {
        assert(res12 === expected12)
    }
}
