package com.hca.cdm.hl7.parser.tests

import com.hca.cdm.hl7.constants.HL7Types
import org.scalatest.FlatSpec

/**
  * Created by cloudera on 3/1/17.
  */
class HL7ParserPPRTest extends FlatSpec{

    val testFileBasePath = "/src/test/scala/com/hca/cdm/hl7/parser/tests"
    val hl7TestSetup = new HL7ParserTestSetup(HL7Types.PPR)
    hl7TestSetup.loadProperties("Hl7TestConfig.properties")


    val messageName1 = "PPR_1"
    val msg1 = HL7ParserTestUtils.getMessage(testFileBasePath, messageName1)
    val res1 = hl7TestSetup.parse(msg1)
    val expected1 = HL7ParserTestUtils.getExpected(testFileBasePath, messageName1)
    "PPR Message Test 1 (PPR_1)" should "have a match for the parsed output" in {
        assert(res1 === expected1)
    }

    val messageName2 = "PPR_2"
    val msg2 = HL7ParserTestUtils.getMessage(testFileBasePath, messageName2)
    val res2 = hl7TestSetup.parse(msg2)
    val expected2 = HL7ParserTestUtils.getExpected(testFileBasePath, messageName2)
    "PPR Message Test 2 (PPR_2)" should "have a match for the parsed output" in {
        assert(res2 === expected2)
    }

    val messageName3 = "PPR_3"
    val msg3 = HL7ParserTestUtils.getMessage(testFileBasePath, messageName3)
    val res3 = hl7TestSetup.parse(msg3)
    val expected3 = HL7ParserTestUtils.getExpected(testFileBasePath, messageName3)
    "PPR Message Test 3 (PPR_3)" should "have a match for the parsed output" in {
        assert(res3 === expected3)
    }

    val messageName4 = "PPR_4"
    val msg4 = HL7ParserTestUtils.getMessage(testFileBasePath, messageName4)
    val res4 = hl7TestSetup.parse(msg4)
    val expected4 = HL7ParserTestUtils.getExpected(testFileBasePath, messageName4)
    "PPR Message Test 4 (PPR_4)" should "have a match for the parsed output" in {
        assert(res4 === expected4)
    }

}
