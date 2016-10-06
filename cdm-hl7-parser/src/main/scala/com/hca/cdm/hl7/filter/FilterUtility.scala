package com.hca.cdm.hl7.filter

import com.hca.cdm.EMPTYSTR
import com.hca.cdm.hl7.constants.HL7Constants._
import com.hca.cdm.utils.Filters.Conditions._
import com.hca.cdm.utils.Filters.Expressions._
import com.hca.cdm.utils.Filters.FILTER

import scala.util.control.Breaks.{break, breakable}
import scala.util.{Success, Try}

/**
  * Created by Devaraj Jonnadula on 9/28/2016.
  */
object FilterUtility {
  private val NOFILTER = FILTER(EMPTYSTR, (EMPTYSTR, EMPTYSTR), (EQUAL, NONE))

  def filterTransaction(filters: Array[FILTER])(data: mapType): Boolean = {
    if (filters isEmpty) return true
    var expression = false
    filters length match {
      case x if x >= 2 =>
        for (index <- filters.indices by 2) {
          val left = Try(filters(index)) match {
            case Success(filter) => filter
            case _ => NOFILTER
          }
          val right = Try(filters(index + 1)) match {
            case Success(filter) => filter
            case _ => NOFILTER
          }
          left != NOFILTER match {
            case true =>
              if (right != NOFILTER) {
                left.filter._2 match {
                  case AND =>
                    expression = matchCriteria(data(findReqSegment(data, left.segment)),
                      left.filter._1, left.path._2, left.matchPath) && matchCriteria(data(findReqSegment(data, right.segment)), right.filter._1, right.path._2, right.matchPath)
                  case OR =>
                    expression = matchCriteria(data(findReqSegment(data, left.segment)),
                      left.filter._1, left.path._2, left.matchPath) || matchCriteria(data(findReqSegment(data, right.segment)), right.filter._1, right.path._2, right.matchPath)
                  case NONE => expression = matchCriteria(data(findReqSegment(data, right.segment)), right.filter._1, right.path._2, right.matchPath)
                }
              } else {
                val leftCond = matchCriteria(data(findReqSegment(data, left.segment)), left.filter._1, left.path._2, left.matchPath)
                val temp = expression
                filters(index - 1).filter._2 match {
                  case AND => expression = temp && leftCond
                  case OR => expression = temp || leftCond
                  case NONE => expression = temp
                }
              }
            case _ =>
          }
        }
      case 1 =>
        val fil = filters(0)
        expression = matchCriteria(data(findReqSegment(data, fil.segment)), fil.filter._1, fil.path._2, fil.matchPath)
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


  private def matchCriteria(data: Any, condition: Condition, toMatch: String, path: Array[String]): Boolean = {
    data match {
      case map: mapType =>
        path headOption match {
          case Some(x) => if (map.isDefinedAt(x)) {
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
      case str: String => matchCondition(condition, toMatch, str)
      /*case immMap: Map[String, Any] => path headOption match {
        case Some(x) => if (immMap.isDefinedAt(x)) {
          immMap(x) match {
            case str: String => matchCondition(condition, toMatch, str)
            case _ => matchCriteria(immMap(x), condition, toMatch, path tail)
          }
        }
        else false
        case _ => false
      }*/
      case _ => false
    }
  }


  private def matchCondition(condition: Condition, toMatch: String, in: String) = {
    condition match {
      case CONTAINS => in contains toMatch
      case EQUAL => toMatch == in
      case NOTEQUAL => toMatch != in
      case GT => toMatch.compareTo(in) > 0
      case LT => toMatch.compareTo(in) < 0
      case GTE => toMatch.compareTo(in) >= 0
      case LTE => toMatch.compareTo(in) <= 0
      case _ => false

    }
  }


}
