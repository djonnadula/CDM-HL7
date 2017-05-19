package com.hca.cdm.hl7.parser.tests

import com.hca.cdm.hl7.constants.HL7Types
import org.scalatest.FlatSpec

/**
  * Created by Peter James on 3/1/2017.
  *
  * RDE Message Parsing Unit Tests
  */
class HL7ParserRDETest extends FlatSpec{

    val messageType = HL7Types.RDE
    val hl7TestSetup = new HL7ParserTestSetup(messageType)
    val testFileBasePath = "/src/test/scala/com/hca/cdm/hl7/parser/tests/"
    hl7TestSetup.loadProperties("Hl7TestConfig.properties")

    // MT.2.2 Test
    val messageName1 = "RDE_1"
    val msg1 = HL7ParserTestUtils.getMessage(testFileBasePath, messageName1, messageType)
    val res1 = hl7TestSetup.parse(msg1)
    val expected1 = HL7ParserTestUtils.getExpected(testFileBasePath, messageName1, messageType)
    "RDE Message Test 1 (RDE_1)" should "have a match for the parsed output" in {
        assert(res1 === expected1)
    }
}
