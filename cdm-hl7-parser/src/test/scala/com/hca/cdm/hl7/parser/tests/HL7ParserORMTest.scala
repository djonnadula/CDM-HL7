package com.hca.cdm.hl7.parser.tests

import com.hca.cdm.hl7.constants.HL7Types
import org.scalatest.FlatSpec

/**
  * Created by Peter James on 3/1/2017.
  *
  * ORM Message Parsing Unit Tests
  */
class HL7ParserORMTest extends FlatSpec{

    val messageType = HL7Types.ORM
    val hl7TestSetup = new HL7ParserTestSetup(messageType)
    val testFileBasePath = "/src/test/scala/com/hca/cdm/hl7/parser/tests/"
    hl7TestSetup.loadProperties("Hl7TestConfig.properties")

    val messageName1 = "ORM_1"
    val msg1 = HL7ParserTestUtils.getMessage(testFileBasePath, messageName1, messageType)
    val res1 = hl7TestSetup.parse(msg1)
    val expected1 = HL7ParserTestUtils.getExpected(testFileBasePath, messageName1, messageType)
    "ORM Message Test 1 (ORM_1)" should "have a match for the parsed output" in {
        assert(res1 === expected1)
    }
}
