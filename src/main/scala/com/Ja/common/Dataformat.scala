package com.Ja.common
import java.util.Calendar

import com.Ja.common.DataFrameFunctions.toStringWithNull
import org.apache.spark.sql.functions._

object Dataformat {

  def toVersion(s: String): String = {
    var retvalue = "v1"
    if (s != null && s != "" && s.toString.indexOf("_") >= 0) {
      val first = s.split(",")(0)
      if (first.toString.indexOf("_") >= 0) {
        retvalue = first.substring(first.toString().lastIndexOf('_') + 1)
      }
    }
    retvalue
  }

  // "markers": "20161207_11:31_v40

  def toRemerged(s: String): String = {
    var retvalue = "v26"
    if (s != null && s != "" && s.toString.indexOf("_") >= 0) {
      val first = s.split(",")(0)
      if (first.toString.indexOf("_") >= 0) {
        retvalue = first
      }
    }
    retvalue
  }

  def toEVersion(s: String): String = {
    var retvalue = "v26"
    if (s != null && s != "" && s.toString.indexOf("_") >= 0) {
      val first = s.split(",")(0)
      if (first.toString.indexOf("_") >= 0) {
        retvalue = first.substring(first.toString().lastIndexOf('_') + 1)
        val retvalue1 = first.substring(first.toString().lastIndexOf('_') + 2).toInt
        val retvalue2 = "v26"
        if (retvalue1 < 26) {
          retvalue = retvalue2
        } else retvalue
      }
    }
    retvalue
  }


  def toDate(s:String) : String = {
    val dat = new java.text.SimpleDateFormat("MM/dd/yyyy HH:mm").parse(s)
    var cal: Calendar = Calendar.getInstance()
    cal.setTime(dat)
    val minuteFormat = new java.text.SimpleDateFormat("dd/MM/yyyy HH:mm").format(cal.getTime)
    //cal.getTime
    minuteFormat
  }



 def todateformat(s: String): String = {
    var retvalue = ""
    //  12/07/2017 hh:mm
    if (s != null && s != "" && s.toString.indexOf("/") >= 0) {

      val V2 =

        (
          s.substring((s.lastIndexOf("/") - 2), (s.lastIndexOf("/") + 1)) +
          s.substring((s.lastIndexOf("/") - 5), (s.lastIndexOf("/") - 3)) + "/" +
          s.substring(s.lastIndexOf("/") + 1, 10)
          )
      retvalue = V2
    }

   retvalue

  }

  val toStringUDF = udf[String, Any](any => if (any == null) null else toStringWithNull(any))
  val toStringversion = udf[String, String] {
    toVersion
  }
  val toStringEversion = udf[String, String] {
    toEVersion
  }
  val toStringremerged = udf[String, String] {
    toRemerged
  }

  val toDateUDF = udf[String, String] {
    toDate
  }
}
