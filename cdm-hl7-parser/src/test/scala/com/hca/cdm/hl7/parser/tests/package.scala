package com.hca.cdm.hl7.parser.tests

import java.io.{BufferedReader, FileReader}
import java.nio.file.Paths

import com.google.gson.JsonParser

/**
  * Created by cloudera on 2/27/17.
  */
package object HL7ParserTestUtils {

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

    def expected(messageLocation: String): String = {
        val currentDir = Paths.get(System.getProperty("user.dir"))
        val msgDir = currentDir.toString + messageLocation
        val jsonReader = new JsonParser
        val msg = jsonReader.parse(new FileReader(msgDir))
        msg.toString
    }

    def createPropertiesFileName(fileName: String): String = {
        fileName + ".properties"
    }

    def createMessageFileName(fileName: String): String = {
        fileName + ".txt"
    }

    def createExpectedFileName(fileName: String): String = {
        fileName + ".json"
    }

    def testFilePath(basePath: String, fileType: String): String = {
        basePath + "/" + fileType + "/"
    }

    def getMessage(testFileBasePath: String, messageName: String): String = {
        message(testFilePath(testFileBasePath, "message") + createMessageFileName(messageName))
    }

    def getExpected(testFileBasePath: String, messageName: String): String = {
        expected(testFilePath(testFileBasePath, "expected") + createExpectedFileName(messageName))
    }

    def getOS: String = {
        System.getProperty("os.name")
    }
}
