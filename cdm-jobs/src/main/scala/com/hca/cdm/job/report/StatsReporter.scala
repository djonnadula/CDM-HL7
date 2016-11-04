package com.hca.cdm.job.report

import java.text.NumberFormat
import com.hca.cdm.hl7.constants.HL7Constants._
import com.hca.cdm.log.Logg
import com.hca.cdm.notification.{EVENT_TIME, sendMail => mail}
import com.hca.cdm.notification.TaskState._
import com.hca.cdm.utils.DateConstants._
import com.hca.cdm.utils.DateUtil._
import java.time.LocalDate.now
import com.hca.cdm.hl7.model._
import com.hca.cdm.hl7.model.SegmentsState._
import com.hca.cdm.job.{HL7Job => job}


/**
  * Created by Devaraj Jonnadula on 9/26/2016.
  *
  * Report Generator for Metrics for This System
  */
class StatsReporter(private val app: String) extends Logg with Runnable {
  private var segmentMetrics: Map[String, Long] = _
  private val builder = new StringBuilder
  private val append = builder append (_: Any)
  private val format = NumberFormat.getNumberInstance
  private val tdData = " <td width=100 style=font-size:1em; line-height:1.2em; font-family:georgia;>"
  private val tdDataEnd = "</td>"
  private val defNotes = new StringBuilder
  SegmentsState.values.foreach {
    case PROCESSED => defNotes append ("<p>" + PROCESSED + " : Hl7 has been Successfully Processed in specific stage and Written to Kafka</p>")
    case SKIPPED => defNotes append ("<p>" + SKIPPED + " : Segment doesn't have atleast one Column to be populated. So this Segments is Skipped</p>")
    case FAILED => defNotes append ("<p>" + FAILED + " : Transaction Failed at specific Stage and this log can be found in Rejected Messages with detail message of Failure at Runtime</p>")
    case INVALID => defNotes append ("<p>" + INVALID + " : Invalid Data has been Received</p>")
    case NOTAPPLICABLE => defNotes append ("<p>" + NOTAPPLICABLE + " : Segment Does not apply for the HL7 Received. This means For Example " +
      "when HL7 comes in and we are trying to pull EVN and this message don't have EVN defined </p>")
    case OVERSIZED => defNotes append ("<p>" + OVERSIZED + " : Record Cannot be handle by Kafka. So in this case data will be routed to appropriate HDFS location. Check for " +
      "Job config for more Details.</p>")
    case FILTERED => defNotes append ("<p>" + FILTERED + " : Special Requests like CDI, SCRI .. requires filtering and what ever criteria defined for them doesn't meet " +
      "for this HL7 and it was Filtered.</p>")
  }
  defNotes append ("<p>" + HL7State.REJECTED + " : Hl7 Doesn't meet the Requirement to Process. So it was Rejected as per Criteria and this log can be found in Rejects Topic</p>")

  override def run(): Unit = {
    this.builder.clear()
    val parserMetrics = job.parserMetrics
    val segmentMetrics = job.segmentMetrics
    job.resetMetrics()
    val parserGrp = parserMetrics groupBy (x => x._1.substring(0, x._1.indexOf(COLON)))
    val segmentsGrp = segmentMetrics groupBy (x => x._1.substring(0, x._1.indexOf(COLON)))
    val date = dateToString(now.minusDays(1), DATE_PATTERN_YYYY_MM_DD)
    val from = dateToString(now.minusDays(2), DATE_PATTERN_YYYY_MM_DD)
    append("</div></div>")
    val parserTable = "<div style=color:#0000FF><h3>Hl7 Messages " + parserGrp.keys.mkString(";") + " Processed from Dates between " + from + " to " + date + " Stats as Follows</h3>" +
      "<br/><table cellspacing=0 cellpadding=10 border=1 style=font-size:1em; line-height:1.2em; font-family:georgia;" +
      "<thead><tr>" +
      "<th width=30 style=font-weight:bold; font-size:1em; line-height:1.2em; font-family:georgia;>" +
      "Message Type</th>" +
      "<th width=60 style=font-weight:bold; font-size:1em; line-height:1.2em; font-family:georgia;>" +
      "Transaction State</th>" +
      "<th width=100 style=font-weight:bold; font-size:1em; line-height:1.2em; font-family:georgia;>" +
      "Metric</th>" +
      "</tr></thead>"
    append(parserTable)
    tableData(parserGrp)
    append("</table> </div>")
    append("</div> </div>")
    val segmentsTable = "<div style=color:#0000FF><h3>Segments for Hl7 Messages " + parserGrp.keys.mkString(";") + " Processed from Dates between " + from + " to " + date +
      " Stats as follows</h3>" +
      "<br/><table cellspacing=0 cellpadding=10 border=1 style=font-size:1em; line-height:1.2em; font-family:georgia;" +
      "<thead><tr>" +
      "<th width=30 style=font-weight:bold; font-size:1em; line-height:1.2em; font-family:georgia;>" +
      "Message Type</th>" +
      "<th width=100 style=font-weight:bold; font-size:1em; line-height:1.2em; font-family:georgia;>" +
      "Segment</th>" +
      "<th width=60 style=font-weight:bold; font-size:1em; line-height:1.2em; font-family:georgia;>" +
      "Transaction State</th>" +
      "<th width=100 style=font-weight:bold; font-size:1em; line-height:1.2em; font-family:georgia;>" +
      "Metric</th>" +
      "</tr></thead>"
    append(segmentsTable)
    tableData(segmentsGrp, segments = true)
    append("</table> </div>")
    append("</table>")
    append("</div> </div>")
    append("<div style=color:#0000FF><h4><mark>Notes : </mark> Definitions for States Defined </h4>" +
      defNotes.toString())
    append("</div>")
    append("</div> </div>")
    append(EVENT_TIME)
    mail(app + "  Statistics for " + date, builder.result(), WARNING, statsReport = true)
  }

  private def tableData(store: Map[String, Map[String, Long]], segments: Boolean = false) = {
    store.toSeq.sortBy(_._1).foreach({ case (hl7, metricStore) =>
      metricStore.toSeq.sortBy(_._1).foreach({ case (state, metric) =>
        if (metric > 0L) {
          append(segments match {
            case true =>
              "<tr>" + tdData + "<strong>" + hl7 + "</strong>" + tdDataEnd +
                tdData + state.substring(state.indexOf(COLON) + 1, state.lastIndexOf(COLON)) + tdDataEnd +
                tdData + state.substring(state.lastIndexOf(COLON) + 1) + tdDataEnd +
                tdData + format.format(metric) + tdDataEnd +
                "</tr>"
            case _ =>
              "<tr>" + tdData + "<strong>" + hl7 + "</strong>" + tdDataEnd +
                tdData + state.substring(state.indexOf(COLON) + 1) + tdDataEnd +
                tdData + format.format(metric) + tdDataEnd +
                "</tr>"
          })
        }
      })
      append("</tr>")
    })
  }
}
