package com.hca.cdm.hl7.parser.tests

import com.hca.cdm.hl7.constants.HL7Types
import org.scalatest.FlatSpec

/**
  * Created by cloudera on 2/14/17.
  */
class HL7ParserADTTest extends FlatSpec {

    val testFileBasePath = "/src/test/scala/com/hca/cdm/hl7/parser/tests"
    val hl7TestSetup = new HL7ParserTestSetup(HL7Types.ADT)
    hl7TestSetup.loadProperties("Hl7TestConfig.properties")


    val messageName1 = "ADT_1"
    val msg1 = HL7ParserTestUtils.message(HL7ParserTestUtils.testFilePath(testFileBasePath, "message") + HL7ParserTestUtils.createMessageFileName(messageName1))
    val res1 = hl7TestSetup.parse(msg1)
    val expected1 = HL7ParserTestUtils.expected(HL7ParserTestUtils.testFilePath(testFileBasePath, "expected") + HL7ParserTestUtils.createExpectedFileName(messageName1))

    "ADT Message Test 1" should "have a match for the parsed output" in {
        assert(res1 === expected1)
    }

    val messageName2 = "ADT_2"
    val msg2 = HL7ParserTestUtils.message(HL7ParserTestUtils.testFilePath(testFileBasePath, "message") + HL7ParserTestUtils.createMessageFileName(messageName2))
    val res2 = hl7TestSetup.parse(msg2)
    val expected2 = HL7ParserTestUtils.expected(HL7ParserTestUtils.testFilePath(testFileBasePath, "expected") + HL7ParserTestUtils.createExpectedFileName(messageName2))
    "ADT Message Test 2" should "have a match for the parsed output" in {
        assert(res2 === expected2)
    }

    val messageName3 = "ADT_3"
    val msg3 = HL7ParserTestUtils.message(HL7ParserTestUtils.testFilePath(testFileBasePath, "message") + HL7ParserTestUtils.createMessageFileName(messageName3))
    val res3 = hl7TestSetup.parse(msg3)
    val expected3 = HL7ParserTestUtils.expected(HL7ParserTestUtils.testFilePath(testFileBasePath, "expected") + HL7ParserTestUtils.createExpectedFileName(messageName3))
    "ADT Message Test 3" should "have a match for the parsed output" in {
        assert(res3 === expected3)
    }

}
