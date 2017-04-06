package com.hca.cdm.hl7.parser.tests

import com.hca.cdm.hl7.constants.HL7Types
import org.scalatest.FlatSpec

/**
  * Created by Peter James on 3/1/2017.
  *
  * SIU Message Parsing Unit Tests
  */
class HL7ParserSIUTest extends FlatSpec{

    val messageType = HL7Types.SIU
    val hl7TestSetup = new HL7ParserTestSetup(messageType)
    val testFileBasePath = "/src/test/scala/com/hca/cdm/hl7/parser/tests/"
    hl7TestSetup.loadProperties("Hl7TestConfig.properties")

    val messageName1 = "SIU_1"
    val msg1 = HL7ParserTestUtils.getMessage(testFileBasePath, messageName1, messageType.toString)
    val res1 = hl7TestSetup.parse(msg1)
    val expected1 = HL7ParserTestUtils.getExpected(testFileBasePath, messageName1, messageType.toString)
    "SIU Message Test 1 (SIU_1)" should "have a match for the parsed output" in {
        assert(res1 === expected1)
    }

    val messageName2 = "SIU_2"
    val msg2 = HL7ParserTestUtils.getMessage(testFileBasePath, messageName2, messageType.toString)
    val res2 = hl7TestSetup.parse(msg2)
    val expected2 = HL7ParserTestUtils.getExpected(testFileBasePath, messageName2, messageType.toString)
    "SIU Message Test 2 (SIU_2)" should "have a match for the parsed output" in {
        assert(res2 === expected2)
    }

    val messageName3 = "SIU_3"
    val msg3 = HL7ParserTestUtils.getMessage(testFileBasePath, messageName3, messageType.toString)
    val res3 = hl7TestSetup.parse(msg3)
    val expected3 = HL7ParserTestUtils.getExpected(testFileBasePath, messageName3, messageType.toString)
    "SIU Message Test 3 (SIU_3)" should "have a match for the parsed output" in {
        assert(res3 === expected3)
    }

    val messageName4 = "SIU_4"
    val msg4 = HL7ParserTestUtils.getMessage(testFileBasePath, messageName4, messageType.toString)
    val res4 = hl7TestSetup.parse(msg4)
    val expected4 = HL7ParserTestUtils.getExpected(testFileBasePath, messageName4, messageType.toString)
    "SIU Message Test 4 (SIU_4)" should "have a match for the parsed output" in {
        assert(res4 === expected4)
    }

    val messageName5 = "SIU_5"
    val msg5 = HL7ParserTestUtils.getMessage(testFileBasePath, messageName5, messageType.toString)
    val res5 = hl7TestSetup.parse(msg5)
    val expected5 = HL7ParserTestUtils.getExpected(testFileBasePath, messageName5, messageType.toString)
    "SIU Message Test 5 (SIU_5)" should "have a match for the parsed output" in {
        assert(res5 === expected5)
    }

    val messageName6 = "SIU_6"
    val msg6 = HL7ParserTestUtils.getMessage(testFileBasePath, messageName6, messageType.toString)
    val res6 = hl7TestSetup.parse(msg6)
    val expected6 = HL7ParserTestUtils.getExpected(testFileBasePath, messageName6, messageType.toString)
    "SIU Message Test 6 (SIU_6)" should "have a match for the parsed output" in {
        assert(res6 === expected6)
    }
}
