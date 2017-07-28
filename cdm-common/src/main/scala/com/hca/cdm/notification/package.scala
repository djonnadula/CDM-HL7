package com.hca.cdm

import java.util.Date
import com.hca.cdm.log.Logg
import org.apache.commons.mail.{EmailException, HtmlEmail, SimpleEmail}
import scala.language.postfixOps

/**
  * Created by Devaraj Jonnadula on 9/19/2016.
  *
  * Sends Emails as per Config
  */
package object notification extends  Logg{


  private lazy val SMTP_HOST = lookUpProp("smtp.host")
  private lazy val emailTo = lookUpProp("notificationList").split(",")
  private lazy val emailFrom = lookUpProp("notificationFrom")
  private lazy val bounceAddress = lookUpProp("bounceNotifier")

  object TaskState extends Enumeration {
    type taskState = Value
    val CRITICAL = Value("CRITICAL")
    val WARNING = Value("WARNING")
    val NORMAL = Value("NORMAL")

  }

  def EVENT_TIME: String = s"Event Triggered time :: ${new Date().toString}"

  import TaskState._

  def sendMail(subject: String, messageBody: String, state: taskState = WARNING, statsReport: Boolean = false, monitorUnit: Array[String] = Array[String]()): Unit = {
    val properties = System.getProperties
    properties.setProperty("mail.smtp.host", SMTP_HOST)
    val mail = if (statsReport) {
      new HtmlEmail
    } else {
      new SimpleEmail
    }
    try {
      state match {
        case CRITICAL => mail.addHeader("X-Priority", "1")
        case WARNING => mail.addHeader("X-Priority", "3")
        case _ =>
      }
      mail setSentDate new Date()
      mail setHostName SMTP_HOST
      mail setBounceAddress bounceAddress
      mail setFrom emailFrom
      if (monitorUnit nonEmpty) {
        monitorUnit foreach (to => mail addTo to)
        emailTo foreach (cc => mail addCc cc)
      } else {
        emailTo foreach (to => mail addTo to)
      }
      mail setSubject subject
      mail setMsg messageBody
      info("Email Sent :: " + mail.send)
    } catch {
      case mex: EmailException => error("Cannot Send Notification :: ", mex)
    }
  }

}
