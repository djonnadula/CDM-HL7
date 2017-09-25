package com.hca.cdm.hl7.parser.tests

import com.hca.cdm.hl7.constants.HL7Types
import com.hca.cdm.log.Logg
import org.scalatest.FlatSpec

/**
  * Created by Peter James on 3/1/2017.
  *
  * RAS Message Parsing Unit Tests
  */
class HL7ParserRASTest extends FlatSpec with Logg {

    val messageType = HL7Types.RAS
    val hl7TestSetup = new HL7ParserTestSetup(messageType)
    val testFileBasePath = "/src/test/scala/com/hca/cdm/hl7/parser/tests/"

    val messageName1 = "RAS_1"
    val msg1 = HL7ParserTestUtils.getMessage(testFileBasePath, messageName1, messageType)
    val res1 = hl7TestSetup.parse(msg1)
    info("RAS Parsed")
    val expected1 = HL7ParserTestUtils.getExpected(testFileBasePath, messageName1, messageType)
    "RAS Message Test 1 (RAS_1)" should "have a match for the parsed output" in {
        assert(res1 === expected1)
    }

    val messageName2 = "RAS_2"
    val msg2 = HL7ParserTestUtils.getMessage(testFileBasePath, messageName2, messageType)
    val res2 = hl7TestSetup.parse(msg2)
    val expected2 = HL7ParserTestUtils.getExpected(testFileBasePath, messageName2, messageType)
    "RAS Message Test 2 (RAS_2)" should "have a match for the parsed output" in {
        assert(res2 === expected2)
    }
}
