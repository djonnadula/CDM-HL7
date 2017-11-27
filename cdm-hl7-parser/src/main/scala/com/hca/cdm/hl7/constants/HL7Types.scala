package com.hca.cdm.hl7.constants

/**
  * Created by Devaraj Jonnadula on 8/12/2016.
  *
  * HL7 Message Types
  */
object HL7Types extends Enumeration {
  self =>
  type HL7 = Value
  val ADT = Value("ADT")
  val DFT = Value("DFT")
  val MDM = Value("MDM")
  val ORU = Value("ORU")
  val PPR = Value("PPR")
  val RAS = Value("RAS")
  val RDE = Value("RDE")
  val SIU = Value("SIU")
  val VXU = Value("VXU")
  val IPLORU = Value("IPLORU")
  val ORM = Value("ORM")
  val ORMORDERS = Value("ORMORDERS")
  val IPLORDERS = Value("IPLORDERS")
  val CRNADT = Value("CRNADT")
  val CRNORU = Value("CRNORU")
  val CRNORM = Value("CRNORM")
  val UNKNOWN = Value("UNKNOWN")
  val ALL = Value("ALL")
  val VENTORU = Value("VENTORU")

   def allKnownHL7 : String = self.values.mkString(":")
}
