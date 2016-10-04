package com.hca.cdm.hl7.validation

import com.hca.cdm._
import com.hca.cdm.hl7.audit.MSGMeta

/**
  * Created by Devaraj Jonnadula on 9/20/2016.
  */
object ValidationUtil {

  def isValidMsg(meta: MSGMeta): Boolean = meta.controlId != EMPTYSTR & meta.facility != EMPTYSTR



}
