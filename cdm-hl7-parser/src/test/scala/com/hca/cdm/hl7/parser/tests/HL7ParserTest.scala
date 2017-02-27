package com.hca.cdm.hl7.parser.tests

import com.hca.cdm.hl7.constants.HL7Types
import org.scalatest.FlatSpec

/**
  * Created by cloudera on 2/14/17.
  */
class HL7ParserTest extends FlatSpec {

    val messageName = "ADT_1"
    val hl7testsetup = new HL7ParserSetup(HL7Types.ADT)
    val msg = hl7testsetup.message("/src/test/scala/com/hca/cdm/hl7/parser/tests/message/" + messageName + ".txt")
    val res = hl7testsetup.parse(msg)
    println("result: \n" + res)
    val expected = hl7testsetup.expected("/src/test/scala/com/hca/cdm/hl7/parser/tests/expected/" + messageName + ".json")
    "ADT Message Test" should "have a match for the parsed output" in {
        assert(res === expected)
    }

}
