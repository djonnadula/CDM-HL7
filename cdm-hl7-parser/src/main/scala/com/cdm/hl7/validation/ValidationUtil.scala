package com.cdm.hl7.validation

import com.cdm.Models.MSGMeta
import com.cdm._
import com.cdm.hl7.model._
import com.cdm.hl7.constants.HL7Constants._

/**
  * Created by Devaraj Jonnadula on 9/20/2016.
  */
object ValidationUtil {

  case class Validation(status: Either[Boolean, (String, String)])

  private val valid = Validation(Left(true))
  private val invalidControlId = Validation(Right("Control Id ", EMPTYSTR))
  private val invalidFacilityMnemonic = Validation(Right(EMPTYSTR, "Facility Mnemonic "))
  private val invalid = Validation(Right(EMPTYSTR, EMPTYSTR))

  def isValidMsg(meta: MSGMeta): Validation = {
    if (meta.controlId != EMPTYSTR && meta.facility != EMPTYSTR) {
      return valid
    } else {
      if (meta.controlId == EMPTYSTR) return invalidControlId
      else if (meta.facility == EMPTYSTR) return invalidFacilityMnemonic
    }
    invalid
  }

  def hasMultiMSH(data: mapType): Boolean = {
    data.count({ case (node, any) =>
      node.substring(node.indexOf(DOT) + 1) == MSH
    }) > 1
  }

}
