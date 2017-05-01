package com.hca.cdm.hl7.overrides

import com.hca.cdm.hl7.model._
import com.hca.cdm.lookUpProp
import scala.collection.mutable
import com.hca.cdm.hl7.constants.HL7Constants.mapType


/**
  * Created by Devaraj Jonnadula on 5/1/2017.
  */

abstract class OverrideHandle {

  def applyOverride(node: mapType): Unit

  def mappings: mutable.HashMap[String, Array[(String, String, String, String)]]

  def isInterfaceSpecific(key: String): Boolean
}


package object overrides {

  case class Interface_GtMriOverride() extends OverrideHandle {
    lazy val GT_MRI_MAPPINGS: mutable.HashMap[String, Array[(String, String, String, String)]] = synchronized(
      commonSegmentMappings(lookUpProp("interface.gtmri.mappings")))

    private lazy val acctNum: String = lookUpProp("interface.gtmri.overrride")

    private lazy val Gt_MRI_RegEx = ".*?(GTMRI).*".r.pattern.matcher(_: String).matches()

    def applyOverride(node: mapType): Unit = {
      node remove acctNum
    }

    override def mappings: mutable.HashMap[String, Array[(String, String, String, String)]] = GT_MRI_MAPPINGS

    override def isInterfaceSpecific(key: String): Boolean = Gt_MRI_RegEx(key)
  }

}
