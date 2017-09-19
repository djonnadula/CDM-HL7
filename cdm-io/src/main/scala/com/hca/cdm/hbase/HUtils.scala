package com.hca.cdm.hbase

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.hca.cdm._
import com.hca.cdm.log.Logg
import com.hca.cdm.utils.RetryHandler
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.{HColumnDescriptor, KeepDeletedCells}
import org.apache.hadoop.hbase.io.compress.Compression
import org.apache.hadoop.hbase.regionserver.BloomType
import org.apache.hadoop.hbase.util.Bytes._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import collection.JavaConverters._

/**
  * Created by Devaraj Jonnadula on 8/23/2017.
  */
object HUtils extends Logg {

  private lazy val Replication_Factor: Short = 3.toShort
  private lazy val DEFAULT_TTL: Int = 1 * 31 * 24 * 60 * 60
  private val mapper = new ObjectMapper().registerModule(DefaultScalaModule).readValue(_: String, classOf[mutable.Map[String, String]])
  private lazy val NO_DATA = mutable.Map[String, String]().empty


  def createFamily(familyName: String, props: Map[String, String] = Map.empty): HColumnDescriptor = {
    val fam = new HColumnDescriptor(familyName)
      .setBlockCacheEnabled(true)
      .setBloomFilterType(BloomType.ROWCOL)
      .setCacheBloomsOnWrite(false)
      .setCacheDataInL1(false)
      .setCacheDataOnWrite(false)
      .setCacheIndexesOnWrite(false)
      .setCompactionCompressionType(Compression.Algorithm.SNAPPY)
      .setCompressTags(true)
      .setDFSReplication(Replication_Factor)
      .setEvictBlocksOnClose(false)
      .setInMemory(false)
      .setKeepDeletedCells(KeepDeletedCells.TTL)
      .setMobEnabled(false)
      .setPrefetchBlocksOnOpen(false)
      .setEvictBlocksOnClose(true)
      .setVersions(1, 10)
      .setTimeToLive(DEFAULT_TTL)
    props.foreach { case (k, v) => fam.setValue(k, v) }
    fam
  }

  def addRowRequest(key: String, family: String, attributes: mutable.Map[String, String]): Put = {
    val row = new Put(toBytes(key))
    val familyBytes = toBytes(family)
    attributes.foreach { case (k, v) => row.addImmutable(familyBytes, toBytes(k), toBytes(v)) }
    row
  }

  def sendRequestFromJson(operator: BatchOperator)(family: String, keys: ListBuffer[String], jsonData: String): Unit = {
    val jsonKV = mapper(jsonData)
    val key = keys.foldLeft(EMPTYSTR)((a, b) => a + jsonKV.getOrElse(b, EMPTYSTR))
    keys.foreach(jsonKV.remove)
    operator.mutate(addRowRequest(key, family, jsonKV))
  }

  def transformRow(table: String, family: String, fetchId: String, fetchAttributes: ListBuffer[String])(operator: HBaseConnector): mutable.Map[String, String] = {
    val response = sendGetRequest(getRowRequest(fetchId, family, fetchAttributes), operator.getTable(table))
    if (valid(response) && !response.isEmpty) {
      response.getFamilyMap(toBytes(family)).asScala.map({
        case (k, v) => (new String(k, UTF8), new String(v, UTF8))
      })
    } else NO_DATA

  }
  def transformRow(table: Any, family: Any, fetchId: Any, fetchAttributes: Any)(operator: HBaseConnector): Any ={
    transformRow(table.asInstanceOf[String],family.asInstanceOf[String],
      fetchId.asInstanceOf[String],fetchAttributes.asInstanceOf[ListBuffer[String]])(operator)
  }

  def sendGetRequest(request: Get, table: Table, retry: Boolean = true): Result = {
    var res: Result = null
    val op = asFunc(res = table.get(request))
    if (retry) tryAndGoNextAction0(new RetryHandler().retryOperation(asFunc(op)), closeResource(table))
    else op()
    res
  }

  def getRowRequest(key: String, family: String, attributes: ListBuffer[String]): Get = {
    val get = new Get(toBytes(key))
    val familyBytes = toBytes(family)
    attributes.foreach { case (qualifier) => get.addColumn(familyBytes, toBytes(qualifier)) }
    get
  }

  def deleteRowRequest(key: String, family: String, attributes: Set[String]): Delete = {
    val delete = new Delete(toBytes(key))
    val familyBytes = toBytes(family)
    attributes.foreach { case (qualifier) => delete.addColumn(familyBytes, toBytes(qualifier)) }
    delete
  }

}
