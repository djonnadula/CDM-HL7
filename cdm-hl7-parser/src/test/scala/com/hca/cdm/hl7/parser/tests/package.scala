package com.hca.cdm.hl7.parser.tests

import java.io.{BufferedReader, FileReader}
import java.nio.file.Paths

import com.google.gson.JsonParser
import com.hca.cdm.hl7.constants.HL7Types.HL7

/**
  * Created by Peter James on 2/27/2017.
  *
  * HL7 Parser Test Utils
  */
package object HL7ParserTestUtils {

    /**
      * Reads an HL7 message from the message directory and returns it as a String
      * @param fileNamePrefix the file name to read
      * @return HL7 message as a String
      */
    def message(fileNamePrefix: String): String = {
        val currentDir = Paths.get(System.getProperty("user.dir"))
        val msgDir = currentDir.toString + fileNamePrefix
        val br = new BufferedReader(new FileReader(msgDir))
        val sb = new StringBuilder
        var line = br.readLine()

        while (line != null) {
            sb.append(line)
            sb.append(System.lineSeparator())
            line = br.readLine()
        }
        val ev = sb.toString
        ev
    }

    /**
      * Reads the expected JSON message and returns it as a String
      * @param messageLocation the file name to read
      * @return JSON message as String
      */
    def expected(messageLocation: String): String = {
        val currentDir = Paths.get(System.getProperty("user.dir"))
        val msgDir = currentDir.toString + messageLocation
        val jsonReader = new JsonParser
        val msg = jsonReader.parse(new FileReader(msgDir))
        msg.toString
    }

    /**
      * Append .properties to end of a String
      * @param fileName file name to add .properties to
      * @return {file_name} + .properties
      */
    def createPropertiesFileName(fileName: String): String = {
        fileName + ".properties"
    }

    /**
      * Append .txt to end of a String
      * @param fileName file name to add .txt to
      * @return {file_name} + .txt
      */
    def createMessageFileName(fileName: String): String = {
        fileName + ".txt"
    }

    /**
      * Append .json to end of a String
      * @param fileName file name to add .json to
      * @return {file_name} + .json
      */
    def createExpectedFileName(fileName: String): String = {
        fileName + ".json"
    }

    /**
      * Create test file path
      * @param basePath starting path
      * @param fileType ending path
      * @param messageType HL7 message type
      * @return starting + ending path
      */
    def testFilePath(basePath: String, fileType: String, messageType: HL7): String = {
        basePath + "/" + fileType + "/" + messageType.toString + "/"
    }

    /**
      * Gets the message from the test message folder
      * @param testFileBasePath test message path
      * @param messageName file name
      * @param messageType HL7 message type
      * @return HL7 message
      */
    def getMessage(testFileBasePath: String, messageName: String, messageType: HL7): String = {
        message(testFilePath(testFileBasePath, "message", messageType) + createMessageFileName(messageName))
    }

    /**
      * Gets the json message from the test message expected folder
      * @param testFileBasePath test json message path
      * @param messageName file name
      * @param messageType HL7 message type
      * @return Json message
      */
    def getExpected(testFileBasePath: String, messageName: String, messageType: HL7): String = {
        expected(testFilePath(testFileBasePath, "expected", messageType) + createExpectedFileName(messageName))
    }

    /**
      * Get the current operating system
      * @return operating system
      */
    def getOS: String = {
        System.getProperty("os.name")
    }
}
