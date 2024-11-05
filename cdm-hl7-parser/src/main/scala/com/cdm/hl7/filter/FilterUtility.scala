package com.cdm.hl7.filter

import com.cdm._
import com.cdm.hl7.constants.HL7Constants._
import com.cdm.utils.Filters.Conditions._
import com.cdm.utils.Filters.Expressions._
import com.cdm.utils.Filters.MultiValues._
import com.cdm.utils.Filters.FILTER
import scala.collection.mutable.ListBuffer
import scala.language.postfixOps

/**
  * Created by Devaraj Jonnadula on 9/28/2016.
  *
  * Utility Filters Data based on Req
  */
object FilterUtility {
  private val NOFILTER = FILTER(EMPTYSTR, (EMPTYSTR, EMPTYSTR), (EQUAL, NONE), None)

  def filterTransaction(filters: Array[FILTER])(data: mapType): Boolean = {
    if (filters isEmpty) return true
    var expression = false
    filters length match {
      case x if x >= 2 =>
        for (index <- filters.indices by 2) {
          val leftFilter = tryAndReturnDefaultValue0(filters(index), NOFILTER)
          val rightFilter = tryAndReturnDefaultValue0(filters(index + 1), NOFILTER)
          if (leftFilter != NOFILTER) {
            if (rightFilter != NOFILTER) {
              val leftExp = multiLocations(findReqSegment(data, leftFilter.segment), data, leftFilter.filter._1, getFilterValues(leftFilter), leftFilter.matchPath)
              val rightExp = multiLocations(findReqSegment(data, rightFilter.segment), data, rightFilter.filter._1, getFilterValues(rightFilter), rightFilter.matchPath)
              leftFilter.filter._2 match {
                case AND =>
                  if (index > 1) {
                    filters(index - 1).filter._2 match {
                      case AND => expression = expression && (leftExp && rightExp)
                      case OR => expression = expression || (leftExp && rightExp)
                    }
                  } else expression = leftExp && rightExp
                case OR =>
                  if (index > 1) {
                    filters(index - 1).filter._2 match {
                      case AND => expression = expression && (leftExp || rightExp)
                      case OR => expression = expression || (leftExp || rightExp)
                    }
                  } else expression = leftExp || rightExp
                case NONE => expression = rightExp
              }
            } else {
              val leftCond = multiLocations(findReqSegment(data, leftFilter.segment), data, leftFilter.filter._1, getFilterValues(leftFilter), leftFilter.matchPath)
              val temp = expression
              filters(index - 1).filter._2 match {
                case AND => expression = temp && leftCond
                case OR => expression = temp || leftCond
                case NONE => expression = temp
              }
            }
          }
        }
      case 1 =>
        val fil = filters(0)
        expression = multiLocations(findReqSegment(data, fil.segment), data, fil.filter._1, getFilterValues(fil), fil.matchPath)

    }
    expression
  }

  private def multiLocations(segs: ListBuffer[String], data: mapType, condition: Condition, toMatch: Either[String, (MultiValueRange, AnyRef)], path: Array[String]): Boolean = {
    segs foreach { seg => if (matchCriteria(data(seg), condition, toMatch, path)) return true }
    false
  }

  private def findReqSegment(data: mapType, reqSeg: String): ListBuffer[String] = {
    data.foldLeft(new ListBuffer[String])((req, node) =>
      if (node._1.substring(node._1.indexOf(".") + 1) == reqSeg) {
        req += node._1
        req
      } else req
    )
  }

  private def getFilterValues(filter: FILTER): Either[String, (MultiValueRange, AnyRef)] = {
    if (filter.multiValues.isDefined) Right(filter.multiValues.get, filter.multipleValues)
    else Left(filter.path._2)
  }


  private def matchCriteria(data: Any, condition: Condition, toMatch: Either[String, (MultiValueRange, AnyRef)], path: Array[String]): Boolean = {
    data match {
      case map: mapType =>
        path headOption match {
          case Some(x) =>
            if (map.isDefinedAt(x)) {
              map(x) match {
                case str: String => matchCondition(condition, toMatch, str)
                case _ => matchCriteria(map(x), condition, toMatch, path tail)
              }
            }
            else false
          case _ => false
        }
      case list: listType =>
        path headOption match {
          case Some(x) =>
            val flatMap = list.flatten.toMap
            if (flatMap isDefinedAt x) {
              flatMap(x) match {
                case str: String => matchCondition(condition, toMatch, str)
                case _ => matchCriteria(flatMap(x), condition, toMatch, path tail)
              }
            }
            else false
          case _ => false
        }
      case str: String =>
        matchCondition(condition, toMatch, str)
      case _ => false
    }
  }


  private def matchCondition(condition: Condition, toMatch: Either[String, (MultiValueRange, AnyRef)], in: String) = {

    toMatch match {
      case Left(toCompare) => compare(condition, toCompare, in)
      case Right(toCompareMulti) =>
        toCompareMulti._1 match {
          case IN =>
            toCompareMulti._2.asInstanceOf[Map[String, String]] isDefinedAt in
          case BETWEEN =>
            toCompareMulti._2.asInstanceOf[Array[String]](0).compareTo(in) > 0 &&
              toCompareMulti._2.asInstanceOf[Array[String]](1).compareTo(in) < 0
        }
    }
  }

  private def compare(condition: Condition, toMatch: String, in: String) = {
    condition match {
      case CONTAINS => in contains toMatch
      case EQUAL => toMatch == in
      case NOTEQUAL => toMatch != in
      case GT => toMatch.compareTo(in) > 0
      case LT => toMatch.compareTo(in) < 0
      case GTE => toMatch.compareTo(in) >= 0
      case LTE => toMatch.compareTo(in) <= 0
      case STARTSWITH => in.startsWith(toMatch)
      case COMPARE => (in compareTo toMatch) == 0
      case COMPARE_IGNORE_CASE_EQUAL => (in compareToIgnoreCase toMatch) == 0
      case _ => false
    }
  }
}
