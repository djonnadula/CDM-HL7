package com.hca.cdm.hl7.parser.tests

import com.hca.cdm.hl7.constants.HL7Types
import org.scalatest.FlatSpec

/**
  * Created by Peter James on 3/1/2017.
  *
  * ORU Message Parsing Unit Tests
  */
class HL7ParserORUTest extends FlatSpec{

    val testFileBasePath = "/src/test/scala/com/hca/cdm/hl7/parser/tests"
    val hl7TestSetup = new HL7ParserTestSetup(HL7Types.ORU)
    hl7TestSetup.loadProperties("Hl7TestConfig.properties")

    val messageName1 = "ORU_1"
    val msg1 = HL7ParserTestUtils.getMessage(testFileBasePath, messageName1)
    val res1 = hl7TestSetup.parse(msg1)
    val expected1 = HL7ParserTestUtils.getExpected(testFileBasePath, messageName1)
    "ORU Message Test 1 (ORU_1)" should "have a match for the parsed output" in {
        assert(res1 === expected1)
    }

    val messageName2 = "ORU_2"
    val msg2 = HL7ParserTestUtils.getMessage(testFileBasePath, messageName2)
    val res2 = hl7TestSetup.parse(msg2)
    val expected2 = HL7ParserTestUtils.getExpected(testFileBasePath, messageName2)
    "ORU Message Test 2 (ORU_2)" should "have a match for the parsed output" in {
        assert(res2 === expected2)
    }

    val messageName3 = "ORU_3"
    val msg3 = HL7ParserTestUtils.getMessage(testFileBasePath, messageName3)
    val res3 = hl7TestSetup.parse(msg3)
    val expected3 = HL7ParserTestUtils.getExpected(testFileBasePath, messageName3)
    "ORU Message Test 3 (ORU_3)" should "have a match for the parsed output" in {
        assert(res3 === expected3)
    }

    val messageName4 = "ORU_4"
    val msg4 = HL7ParserTestUtils.getMessage(testFileBasePath, messageName4)
    val res4 = hl7TestSetup.parse(msg4)
    val expected4 = HL7ParserTestUtils.getExpected(testFileBasePath, messageName4)
    "ORU Message Test 4 (ORU_4)" should "have a match for the parsed output" in {
        assert(res4 === expected4)
    }

    val messageName5 = "ORU_5"
    val msg5 = HL7ParserTestUtils.getMessage(testFileBasePath, messageName5)
    val res5 = hl7TestSetup.parse(msg5)
    val expected5 = HL7ParserTestUtils.getExpected(testFileBasePath, messageName5)
    "ORU Message Test 5 (ORU_5)" should "have a match for the parsed output" in {
        assert(res5 === expected5)
    }

    val messageName6 = "ORU_6"
    val msg6 = HL7ParserTestUtils.getMessage(testFileBasePath, messageName6)
    val res6 = hl7TestSetup.parse(msg6)
    val expected6 = HL7ParserTestUtils.getExpected(testFileBasePath, messageName6)
    "ORU Message Test 6 (ORU_6)" should "have a match for the parsed output" in {
        assert(res6 === expected6)
    }

    val messageName7 = "ORU_7"
    val msg7 = HL7ParserTestUtils.getMessage(testFileBasePath, messageName7)
    val res7 = hl7TestSetup.parse(msg7)
    val expected7 = HL7ParserTestUtils.getExpected(testFileBasePath, messageName7)
    "ORU Message Test 7 (ORU_7)" should "have a match for the parsed output" in {
        assert(res7 === expected7)
    }
}
