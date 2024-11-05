package com.cdm.hl7.parser.tests

import com.cdm.hl7.constants.HL7Types
import com.cdm.log.Logg
import org.scalatest.FlatSpec

/**
  * Created by Peter James on 3/1/2017.
  *
  * VXU Message Parsing Unit Tests
  */
class HL7ParserVXUTest extends FlatSpec with Logg {

    val messageType = HL7Types.VXU
    val hl7TestSetup = new HL7ParserTestSetup(messageType)
    val testFileBasePath = "/src/test/scala/com/hca/cdm/hl7/parser/tests/"

    val messageName1 = "VXU_1"
    val msg1 = HL7ParserTestUtils.getMessage(testFileBasePath, messageName1, messageType)
    val res1 = hl7TestSetup.parse(msg1)
    val expected1 = HL7ParserTestUtils.getExpected(testFileBasePath, messageName1, messageType)
    "VXU Message Test 1 (VXU_1)" should "have a match for the parsed output" in {
        assert(res1 === expected1)
    }
}
