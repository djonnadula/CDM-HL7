package com.cdm.hl7.parser.tests

import com.cdm.hl7.constants.HL7Types
import com.cdm.log.Logg
import org.scalatest.FlatSpec

/**
 *
  * ORM Message Parsing Unit Tests
  */
class HL7ParserORMTest extends FlatSpec with Logg {

    val messageType = HL7Types.ORM
    val hl7TestSetup = new HL7ParserTestSetup(messageType)
    val testFileBasePath = "/src/test/scala/com//cdm/hl7/parser/tests/"

    val messageName1 = "ORM_1"
    val msg1 = HL7ParserTestUtils.getMessage(testFileBasePath, messageName1, messageType)
    val res1 = hl7TestSetup.parse(msg1)
    val expected1 = HL7ParserTestUtils.getExpected(testFileBasePath, messageName1, messageType)
    "ORM Message Test 1 (ORM_1)" should "have a match for the parsed output" in {
        assert(res1 === expected1)
    }

    val messageName2 = "ORM_2"
    val msg2 = HL7ParserTestUtils.getMessage(testFileBasePath, messageName2, messageType)
    val res2 = hl7TestSetup.parse(msg2)
    val expected2 = HL7ParserTestUtils.getExpected(testFileBasePath, messageName2, messageType)
    "ORM Message Test 2 (ORM_2)" should "have a match for the parsed output" in {
        assert(res2 === expected2)
    }
}
