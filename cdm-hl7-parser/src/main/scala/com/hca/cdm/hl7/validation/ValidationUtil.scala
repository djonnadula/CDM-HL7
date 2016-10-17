package com.hca.cdm.hl7.validation

import com.hca.cdm._
import com.hca.cdm.hl7.audit.MSGMeta
import com.hca.cdm.hl7.constants.HL7Constants._

/**
  * Created by Devaraj Jonnadula on 9/20/2016.
  */
object ValidationUtil {

  def isValidMsg(meta: MSGMeta): Boolean = meta.controlId != EMPTYSTR & meta.facility != EMPTYSTR

  def hasMultiMSH(data: mapType): Boolean = {
    data.count({ case (node, any) =>
      node.substring(node.indexOf(".") + 1) == MSH
    }) > 1
  }

}
