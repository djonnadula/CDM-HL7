package com.hca.cdm.hl7.parser.tests

import com.hca.cdm.hl7.constants.HL7Types
import org.scalatest.FlatSpec

/**
  * Created by cloudera on 3/1/17.
  */
class HL7ParserSIUTest extends FlatSpec{

    val testFileBasePath = "/src/test/scala/com/hca/cdm/hl7/parser/tests"
    val hl7TestSetup = new HL7ParserTestSetup(HL7Types.SIU)
    hl7TestSetup.loadProperties("Hl7TestConfig.properties")


    val messageName1 = "SIU_1"
    val msg1 = HL7ParserTestUtils.getMessage(testFileBasePath, messageName1)
    val res1 = hl7TestSetup.parse(msg1)
    val expected1 = HL7ParserTestUtils.getExpected(testFileBasePath, messageName1)
    "SIU Message Test 1 (SIU_1)" should "have a match for the parsed output" in {
        assert(res1 === expected1)
    }

}
