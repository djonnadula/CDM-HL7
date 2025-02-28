package com.cdm.hl7.parser.tests

import com.cdm.hl7.constants.HL7Types
import com.cdm.log.Logg
import org.scalatest.FlatSpec

/**
  * Created by Peter James on 2/14/2017.
  *
  * ADT Message Parsing Unit Tests
  */
class HL7ParserADTTest extends FlatSpec with Logg {

    val messageType = HL7Types.ADT
    val hl7TestSetup = new HL7ParserTestSetup(messageType)
    val testFileBasePath = "/src/test/scala/com//cdm/hl7/parser/tests/"

    val messageName1 = "ADT_1"
    val msg1 = HL7ParserTestUtils.getMessage(testFileBasePath, messageName1, messageType)
    val res1 = hl7TestSetup.parse(msg1)
    val expected1 = HL7ParserTestUtils.getExpected(testFileBasePath, messageName1, messageType)
    "ADT Message Test 1 (ADT_1)" should "have a match for the parsed output" in {
        assert(res1 === expected1)
    }

    // MT 2.5 test
    val messageName2 = "ADT_2"
    val msg2 =  HL7ParserTestUtils.getMessage(testFileBasePath, messageName2, messageType)
    val res2 = hl7TestSetup.parse(msg2)
    val expected2 =  HL7ParserTestUtils.getExpected(testFileBasePath, messageName2, messageType)
    "ADT Message Test 2 (ADT_2)" should "have a match for the parsed output" in {
        assert(res2 === expected2)
    }

    val messageName3 = "ADT_3"
    val msg3 = HL7ParserTestUtils.getMessage(testFileBasePath, messageName3, messageType)
    val res3 = hl7TestSetup.parse(msg3)
    val expected3 = HL7ParserTestUtils.getExpected(testFileBasePath, messageName3, messageType)
    "ADT Message Test 3 (ADT_3)" should "have a match for the parsed output" in {
        assert(res3 === expected3)
    }

    val messageName4 = "ADT_4"
    val msg4 = HL7ParserTestUtils.getMessage(testFileBasePath, messageName4, messageType)
    val res4 = hl7TestSetup.parse(msg4)
    val expected4 = HL7ParserTestUtils.getExpected(testFileBasePath, messageName4, messageType)
    "ADT Message Test 4 (ADT_4)" should "have a match for the parsed output" in {
        assert(res4 === expected4)
    }

    val messageName5 = "ADT_5"
    val msg5 = HL7ParserTestUtils.getMessage(testFileBasePath, messageName5, messageType)
    val res5 = hl7TestSetup.parse(msg5)
    val expected5 = HL7ParserTestUtils.getExpected(testFileBasePath, messageName5, messageType)
    "ADT Message Test 5 (ADT_5)" should "have a match for the parsed output" in {
        assert(res5 === expected5)
    }

    val messageName6 = "ADT_6"
    val msg6 = HL7ParserTestUtils.getMessage(testFileBasePath, messageName6, messageType)
    val res6 = hl7TestSetup.parse(msg6)
    val expected6 = HL7ParserTestUtils.getExpected(testFileBasePath, messageName6, messageType)
    "ADT Message Test 6 (ADT_6)" should "have a match for the parsed output" in {
        assert(res6 === expected6)
    }

    val messageName7 = "ADT_7"
    val msg7 = HL7ParserTestUtils.getMessage(testFileBasePath, messageName7, messageType)
    val res7 = hl7TestSetup.parse(msg7)
    val expected7 = HL7ParserTestUtils.getExpected(testFileBasePath, messageName7, messageType)
    "ADT Message Test 7 (ADT_7)" should "have a match for the parsed output" in {
        assert(res7 === expected7)
    }

    val messageName8 = "ADT_8"
    val msg8 = HL7ParserTestUtils.getMessage(testFileBasePath, messageName8, messageType)
    val res8 = hl7TestSetup.parse(msg8)
    val expected8 = HL7ParserTestUtils.getExpected(testFileBasePath, messageName8, messageType)
    "ADT Message Test 8 (ADT_8)" should "have a match for the parsed output" in {
        assert(res8 === expected8)
    }

    val messageName9 = "ADT_9"
    val msg9 = HL7ParserTestUtils.getMessage(testFileBasePath, messageName9, messageType)
    val res9 = hl7TestSetup.parse(msg9)
    val expected9 = HL7ParserTestUtils.getExpected(testFileBasePath, messageName9, messageType)
    "ADT Message Test 9 (ADT_9)" should "have a match for the parsed output" in {
        assert(res9 === expected9)
    }

    val messageName10 = "ADT_10"
    val msg10 = HL7ParserTestUtils.getMessage(testFileBasePath, messageName10, messageType)
    val res10 = hl7TestSetup.parse(msg10)
    val expected10 = HL7ParserTestUtils.getExpected(testFileBasePath, messageName10, messageType)
    "ADT Message Test 10 (ADT_10)" should "have a match for the parsed output" in {
        assert(res10 === expected10)
    }

    val messageName11 = "ADT_11"
    val msg11 = HL7ParserTestUtils.getMessage(testFileBasePath, messageName11, messageType)
    val res11 = hl7TestSetup.parse(msg11)
    val expected11 = HL7ParserTestUtils.getExpected(testFileBasePath, messageName11, messageType)
    "ADT Message Test 11 (ADT_11)" should "have a match for the parsed output" in {
        assert(res11 === expected11)
    }

    val messageName12 = "ADT_12"
    val msg12 = HL7ParserTestUtils.getMessage(testFileBasePath, messageName12, messageType)
    val res12 = hl7TestSetup.parse(msg12)
    val expected12 = HL7ParserTestUtils.getExpected(testFileBasePath, messageName12, messageType)
    "ADT Message Test 12 (ADT_12)" should "have a match for the parsed output" in {
        assert(res12 === expected12)
    }

    val messageName13 = "ADT_13"
    val msg13 = HL7ParserTestUtils.getMessage(testFileBasePath, messageName13, messageType)
    val res13 = hl7TestSetup.parse(msg13)
    val expected13 = HL7ParserTestUtils.getExpected(testFileBasePath, messageName13, messageType)
    "ADT Message Test 13 (ADT_13)" should "have a match for the parsed output" in {
        assert(res13 === expected13)
    }

    val messageName14 = "ADT_14"
    val msg14 = HL7ParserTestUtils.getMessage(testFileBasePath, messageName14, messageType)
    val res14 = hl7TestSetup.parse(msg14)
    val expected14 = HL7ParserTestUtils.getExpected(testFileBasePath, messageName14, messageType)
    "ADT Message Test 14 (ADT_14)" should "have a match for the parsed output" in {
        assert(res14 === expected14)
    }

    val messageName15 = "ADT_15"
    val msg15 = HL7ParserTestUtils.getMessage(testFileBasePath, messageName15, messageType)
    val res15 = hl7TestSetup.parse(msg15)
    val expected15 = HL7ParserTestUtils.getExpected(testFileBasePath, messageName15, messageType)
    "ADT Message Test 15 (ADT_15)" should "have a match for the parsed output" in {
        assert(res15 === expected15)
    }

    val messageName16 = "ADT_16"
    val msg16 = HL7ParserTestUtils.getMessage(testFileBasePath, messageName16, messageType)
    val res16 = hl7TestSetup.parse(msg16)
    val expected16 = HL7ParserTestUtils.getExpected(testFileBasePath, messageName16, messageType)
    "ADT Message Test 16 (ADT_16)" should "have a match for the parsed output" in {
        assert(res16 === expected16)
    }

    val messageName17 = "ADT_17"
    val msg17 = HL7ParserTestUtils.getMessage(testFileBasePath, messageName17, messageType)
    val res17 = hl7TestSetup.parse(msg17)
    val expected17 = HL7ParserTestUtils.getExpected(testFileBasePath, messageName17, messageType)
    "ADT Message Test 17 (ADT_17)" should "have a match for the parsed output" in {
        assert(res17 === expected17)
    }

    val messageName18 = "ADT_18"
    val msg18 = HL7ParserTestUtils.getMessage(testFileBasePath, messageName18, messageType)
    val res18 = hl7TestSetup.parse(msg18)
    val expected18 = HL7ParserTestUtils.getExpected(testFileBasePath, messageName18, messageType)
    "ADT Message Test 18 (ADT_18)" should "have a match for the parsed output" in {
        assert(res18 === expected18)
    }

    val messageName19 = "ADT_19"
    val msg19 = HL7ParserTestUtils.getMessage(testFileBasePath, messageName19, messageType)
    val res19 = hl7TestSetup.parse(msg19)
    val expected19 = HL7ParserTestUtils.getExpected(testFileBasePath, messageName19, messageType)
    "ADT Message Test 19 (ADT_19)" should "have a match for the parsed output" in {
        assert(res19 === expected19)
    }

    val messageName20 = "ADT_20"
    val msg20 = HL7ParserTestUtils.getMessage(testFileBasePath, messageName20, messageType)
    val res20 = hl7TestSetup.parse(msg20)
    val expected20 = HL7ParserTestUtils.getExpected(testFileBasePath, messageName20, messageType)
    "ADT Message Test 20 (ADT_20)" should "have a match for the parsed output" in {
        assert(res20 === expected20)
    }

    val messageName21 = "ADT_21"
    val msg21 = HL7ParserTestUtils.getMessage(testFileBasePath, messageName21, messageType)
    val res21 = hl7TestSetup.parse(msg21)
    val expected21 = HL7ParserTestUtils.getExpected(testFileBasePath, messageName21, messageType)
    "ADT Message Test 21 (ADT_21)" should "have a match for the parsed output" in {
        assert(res21 === expected21)
    }

    val messageName22 = "ADT_22"
    val msg22 = HL7ParserTestUtils.getMessage(testFileBasePath, messageName22, messageType)
    val res22 = hl7TestSetup.parse(msg22)
    val expected22 = HL7ParserTestUtils.getExpected(testFileBasePath, messageName22, messageType)
    "ADT Message Test 22 (ADT_22)" should "have a match for the parsed output" in {
        assert(res22 === expected22)
    }

    val messageName23 = "ADT_23"
    val msg23 = HL7ParserTestUtils.getMessage(testFileBasePath, messageName23, messageType)
    val res23 = hl7TestSetup.parse(msg23)
    val expected23 = HL7ParserTestUtils.getExpected(testFileBasePath, messageName23, messageType)
    "ADT Message Test 23 (ADT_23)" should "have a match for the parsed output" in {
        assert(res23 === expected23)
    }

    val messageName24 = "ADT_24"
    val msg24 = HL7ParserTestUtils.getMessage(testFileBasePath, messageName24, messageType)
    val res24 = hl7TestSetup.parse(msg24)
    val expected24 = HL7ParserTestUtils.getExpected(testFileBasePath, messageName24, messageType)
    "ADT Message Test 24 (ADT_24)" should "have a match for the parsed output" in {
        assert(res24 === expected24)
    }

    val messageName25 = "ADT_25"
    val msg25 = HL7ParserTestUtils.getMessage(testFileBasePath, messageName25, messageType)
    val res25 = hl7TestSetup.parse(msg25)
    val expected25 = HL7ParserTestUtils.getExpected(testFileBasePath, messageName25, messageType)
    "ADT Message Test 25 (ADT_25)" should "have a match for the parsed output" in {
        assert(res25 === expected25)
    }

    val messageName26 = "ADT_26"
    val msg26 = HL7ParserTestUtils.getMessage(testFileBasePath, messageName26, messageType)
    val res26 = hl7TestSetup.parse(msg26)
    val expected26 = HL7ParserTestUtils.getExpected(testFileBasePath, messageName26, messageType)
    "ADT Message Test 26 (ADT_26)" should "have a match for the parsed output" in {
        assert(res26 === expected26)
    }

    val messageName27 = "ADT_27"
    val msg27 = HL7ParserTestUtils.getMessage(testFileBasePath, messageName27, messageType)
    val res27 = hl7TestSetup.parse(msg27)
    val expected27 = HL7ParserTestUtils.getExpected(testFileBasePath, messageName27, messageType)
    "ADT Message Test 27 (ADT_27)" should "have a match for the parsed output" in {
        assert(res27 === expected27)
    }

    //EPIC.2.3 MRG 4.3 && 4.2
    val messageName28 = "ADT_28"
    val msg28 = HL7ParserTestUtils.getMessage(testFileBasePath, messageName28, messageType)
    val res28 = hl7TestSetup.parse(msg28)
    val expected28 = HL7ParserTestUtils.getExpected(testFileBasePath, messageName28, messageType)
    "ADT Message Test 28 (ADT_28)" should "have a match for the parsed output" in {
        assert(res28 === expected28)
    }

    //MT.2.1 Test
    val messageName29 = "ADT_29"
    val msg29 = HL7ParserTestUtils.getMessage(testFileBasePath, messageName29, messageType)
    val res29 = hl7TestSetup.parse(msg29)
    val expected29 = HL7ParserTestUtils.getExpected(testFileBasePath, messageName29, messageType)
    "ADT Message Test 29 (ADT_29)" should "have a match for the parsed output" in {
        assert(res29 === expected29)
    }

    //Missing ADT ACC data test
    val messageName30 = "ADT_30"
    val msg30 = HL7ParserTestUtils.getMessage(testFileBasePath, messageName30, messageType)
    val res30 = hl7TestSetup.parse(msg30)
    val expected30 = HL7ParserTestUtils.getExpected(testFileBasePath, messageName30, messageType)
    "ADT Message Test 30 (ADT_30)" should "have a match for the parsed output" in {
        assert(res30 === expected30)
    }

    //EPIC.2.3 AL1.7, AL1.8 Test
    val messageName31 = "ADT_31"
    val msg31 = HL7ParserTestUtils.getMessage(testFileBasePath, messageName31, messageType)
    val res31 = hl7TestSetup.parse(msg31)
    val expected31 = HL7ParserTestUtils.getExpected(testFileBasePath, messageName31, messageType)
    "ADT Message Test 31 (ADT_31)" should "have a match for the parsed output" in {
        assert(res31 === expected31)
    }

}
