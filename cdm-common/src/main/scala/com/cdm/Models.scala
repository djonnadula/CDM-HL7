package com.cdm

/**
  * Created by Devaraj Jonnadula on 2/1/2017.
  */
object Models {

  case class MSGMeta(controlId: String, msgCreateTime: String, medical_record_num: String, medical_record_urn: String, account_num: String, facility: String = EMPTYSTR
                     , triggerEvent: String = EMPTYSTR)

}
