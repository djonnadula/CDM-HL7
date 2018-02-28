package com.hca.cdm.hbase

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.hca.cdm._
import com.hca.cdm.log.Logg
import com.hca.cdm.utils.RetryHandler
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.{PageFilter, RandomRowFilter}
import org.apache.hadoop.hbase.{HColumnDescriptor, KeepDeletedCells}
import org.apache.hadoop.hbase.io.compress.Compression
import org.apache.hadoop.hbase.regionserver.BloomType
import org.apache.hadoop.hbase.util.Bytes._
import scala.collection.mutable
import collection.JavaConverters._

/**
  * Created by Devaraj Jonnadula on 8/23/2017.
  */
object HUtils extends Logg {

  private lazy val Replication_Factor: Short = 3.toShort
  private lazy val DEFAULT_TTL: Int = 1 * 31 * 24 * 60 * 60
  private val mapper = new ObjectMapper().registerModule(DefaultScalaModule).readValue(_: String, classOf[mutable.Map[String, String]])
  private lazy val NO_DATA = mutable.Map[String, Array[Byte]]().empty


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
    props foreach { case (k, v) => fam.setValue(k, v) }
    fam
  }

  def addRowRequest(key: String, family: String, attributes: mutable.Map[String, String]): Put = {
    val row = new Put(toBytes(key))
    val familyBytes = toBytes(family)
    attributes foreach { case (k, v) => row.addImmutable(familyBytes, toBytes(k), toBytes(v)) }
    row
  }

  def sendRequestFromJson(operator: BatchOperator)(family: String, key: String, jsonData: String, filter: Boolean = true): Unit = {
    val jsonKV = mapper(jsonData)
    // val key = keys.foldLeft(EMPTYSTR)((a, b) => a + jsonKV.getOrElse(b, EMPTYSTR))
    // keys foreach jsonKV.remove
    if (filter) filterData(jsonKV)
    operator mutate addRowRequest(key, family, jsonKV)
  }

  def sendRequest(operator: BatchOperator)(family: String, key: String, kv: mutable.Map[String, String], filter: Boolean = true): Unit = {
    // val key = keys.foldLeft(EMPTYSTR)((a, b) => a + kv.getOrElse(b, EMPTYSTR))
    if (filter) filterData(kv)
    operator mutate addRowRequest(key, family, kv)
  }

  private def filterData(data: mutable.Map[String, String]): mutable.Map[String, String] = {
    data foreach { case (k, v) => if (v == EMPTYSTR) data -= k }
    data
  }

  def getRow(table: String, family: String, fetchId: String, fetchAttributes: Set[String] = Set.empty[String])(operator: HBaseConnector): mutable.Map[String, Array[Byte]] = {
    val response = sendGetRequest(getRowRequest(fetchId, family, fetchAttributes), operator.getTable(table))
    if (valid(response) && !response.isEmpty) {
      response.getFamilyMap(toBytes(family)).asScala.map({
        case (k, v) => (new String(k, UTF8), v)
      })
    } else NO_DATA

  }

  def getRandom(table: String, family: String, fetchAttributes: Set[String] = Set.empty[String],
                maxMessages: Int = 10, keyFrom: String, keyTo: String)(operator: HBaseConnector): Map[Int, mutable.Map[String, Array[Byte]]] = {
    val familyBytes = toBytes(family)
    val scan = new Scan(toBytes(keyFrom), toBytes(keyTo))
    scan.setCacheBlocks(false)
    scan.setCaching(0)
    scan.setMaxVersions(1)
    scan.setScanMetricsEnabled(false)
    scan.addFamily(familyBytes)
      .setId(s"$table-$family")
    scan.setFilter(new PageFilter(maxMessages))
    fetchAttributes.foreach { case (qualifier) => scan.addColumn(familyBytes, toBytes(qualifier)) }
    val Tab = operator.getTable(table)
    val res = Tab.getScanner(scan)
    var msgs = res.next(maxMessages)
    if (msgs.isEmpty) {
      val s = new Scan
      s.addFamily(familyBytes)
      s.setFilter(new PageFilter(maxMessages))
      msgs = Tab.getScanner(s).next(maxMessages)
    }
    val out = tryAndGoNextAction0(msgs.zipWithIndex.map { case (r, index) => index -> r.getFamilyMap(familyBytes).asScala.map { case (k, v) => toStringBinary(k) -> v } }, closeResource(res)).toMap
    closeResource(Tab)
    out
  }

  def sendGetRequest(request: Get, table: Table, retry: Boolean = true): Result = {
    var res: Result = null

    def op(): Unit = {
      res = table.get(request)
    }

    if (retry) tryAndGoNextAction0(new RetryHandler().retryOperation(op), closeResource(table))
    else tryAndGoNextAction0(op(), closeResource(table))
    res

  }

  def getRowRequest(key: String, family: String, attributes: Set[String]): Get = {
    val get = new Get(toBytes(key))
    get.setIsolationLevel(IsolationLevel.READ_UNCOMMITTED)
    get.setId(key)
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
