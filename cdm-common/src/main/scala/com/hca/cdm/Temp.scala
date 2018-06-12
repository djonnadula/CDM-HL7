/*
package com.hca.cdm

import com.google.api.client.http.FileContent
import com.google.api.services.drive.Drive
import com.google.api.services.drive.model.File
import java.io.IOException


/**
  * Created by Devaraj Jonnadula on 2/8/2018.
  */
object Temp extends  App {
 /* val reader =  Source.fromFile(args(0))

  val writer = new BufferedWriter(new FileWriter(new File(args(1))))
  val ssn_R = "[0-9]{3}[-][0-9]{2}[-][0-9]{4}"
   // "\\s[0-9]{3}[-][0-9]{2}[-][0-9]{4}\\s"
  reader.getLines().foreach{x =>
    writer.write(x.replaceAll(ssn_R, ""))
    writer.newLine()
  }
//  scp /data/raid10/cdm/hl7/Dr/COSMOS/Final.json pzi7542@corpkvs3771k:/D:/Cosmos

  writer.flush()
  writer.close()
*/

  val fileMetadata = new Nothing
  fileMetadata.setName("My Report")
  fileMetadata.setMimeType("application/vnd.google-apps.spreadsheet")

  val filePath = new File("files/report.csv")
  val mediaContent = new Nothing("text/csv", filePath)
  val file = driveService.files.create(fileMetadata, mediaContent).setFields("id").execute
  System.out.println("File ID: " + file.getId)
}
*/
