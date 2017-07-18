package com.hca.cdm.hl7.filter

import com.hca.cdm.EMPTYSTR
import com.hca.cdm.hl7.constants.HL7Constants._
import com.hca.cdm.utils.Filters.Conditions._
import com.hca.cdm.utils.Filters.Expressions._
import com.hca.cdm.utils.Filters.MultiValues._
import com.hca.cdm.utils.Filters.FILTER
import scala.language.postfixOps
import scala.util.control.Breaks.{break, breakable}
import scala.util.{Success, Try}

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
          val leftFilter = Try(filters(index)) match {
            case Success(filter) => filter
            case _ => NOFILTER
          }
          val rightFilter = Try(filters(index + 1)) match {
            case Success(filter) => filter
            case _ => NOFILTER
          }
          if (leftFilter != NOFILTER) {
            if (rightFilter != NOFILTER) {
              val leftExp = findReqSegment(data, leftFilter.segment) match {
                case EMPTYSTR => false
                case seg => matchCriteria(data(seg), leftFilter.filter._1, getFilterValues(leftFilter), leftFilter.matchPath)
              }
              val rightExp = findReqSegment(data, rightFilter.segment) match {
                case EMPTYSTR => false
                case seg => matchCriteria(data(seg), rightFilter.filter._1, getFilterValues(rightFilter), rightFilter.matchPath)
              }
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
              val leftCond = findReqSegment(data, leftFilter.segment) match {
                case EMPTYSTR => false
                case seg => matchCriteria(data(seg), leftFilter.filter._1, getFilterValues(leftFilter), leftFilter.matchPath)
              }
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
        findReqSegment(data, fil.segment) match {
          case EMPTYSTR =>
          case x => expression = matchCriteria(data(x), fil.filter._1, getFilterValues(fil), fil.matchPath)
        }

    }
    expression
  }

  private def findReqSegment(data: mapType, reqSeg: String): String = {
    var segment = EMPTYSTR
    breakable {
      data.foreach(node => {
        if (node._1.substring(node._1.indexOf(".") + 1) == reqSeg) {
          segment = node._1
          break
        }
      })
    }
    segment
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
