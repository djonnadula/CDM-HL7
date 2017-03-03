package com.hca.cdm.hl7.parser.tests

import com.hca.cdm.hl7.constants.HL7Types
import org.scalatest.FlatSpec

/**
  * Created by cloudera on 3/1/17.
  */
class HL7ParserRASTest extends FlatSpec{

    val testFileBasePath = "/src/test/scala/com/hca/cdm/hl7/parser/tests"
    val hl7TestSetup = new HL7ParserTestSetup(HL7Types.RAS)
    hl7TestSetup.loadProperties("Hl7TestConfig.properties")


    val messageName1 = "RAS_1"
    val msg1 = HL7ParserTestUtils.getMessage(testFileBasePath, messageName1)
    val res1 = hl7TestSetup.parse(msg1)
    val expected1 = HL7ParserTestUtils.getExpected(testFileBasePath, messageName1)
    "RAS Message Test 1 (RAS_1)" should "have a match for the parsed output" in {
        assert(res1 === expected1)
    }

    val messageName2 = "RAS_2"
    val msg2 = HL7ParserTestUtils.getMessage(testFileBasePath, messageName2)
    val res2 = hl7TestSetup.parse(msg2)
    val expected2 = HL7ParserTestUtils.getExpected(testFileBasePath, messageName2)
    "RAS Message Test 2 (RAS_2)" should "have a match for the parsed output" in {
        assert(res2 === expected2)
    }

}
