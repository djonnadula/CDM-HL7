package com.hca.cdm.hbase

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.hca.cdm._
import com.hca.cdm.log.Logg
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.{HColumnDescriptor, KeepDeletedCells}
import org.apache.hadoop.hbase.io.compress.Compression
import org.apache.hadoop.hbase.regionserver.BloomType
import org.apache.hadoop.hbase.util.Bytes._
import collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by Devaraj Jonnadula on 8/23/2017.
  */
object HUtils extends Logg{

  private lazy val Replication_Factor: Int = 3
  private lazy val TTL: Int = 1 * 31 * 24 * 60 * 60
  private val EMPTY = mutable.Map.empty[String, String]
  private val mapper = new ObjectMapper().registerModule(DefaultScalaModule).readValue(_: String, classOf[mutable.Map[String, String]])


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
      .setDFSReplication(3)
      .setEvictBlocksOnClose(false)
      .setInMemory(false)
      .setKeepDeletedCells(KeepDeletedCells.TTL)
      .setMobEnabled(false)
      .setPrefetchBlocksOnOpen(false)
      .setEvictBlocksOnClose(true)
      .setVersions(1, 10)
      .setTimeToLive(TTL)
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

  def getRow[T](request: T, operator: (T) => Result): mutable.Map[String, String] = {

    val response = operator(request)

    //if(valid(response)) response.getFamilyMap(request.getf) asScala.map{case(k,v) => (new String(k),new String(v)) }
    //else
    EMPTY
  }

  def getRowRequest(key: String, family: String, attributes: Set[String]): Get = {
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


  def persistRow(mut: Put)(operator: Either[Table, BatchOperator]): Unit = {


  }


}
