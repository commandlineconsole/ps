package com.Ja

/**
  * Created by RCH28 on 29/03/2017.
  */
package object domain {
  /*case class RDDXMLOutput1 (version:String,remerged:String,surveyid:String, fieldtype:String,qlabel:String,qtitle:String,qalttitle:String,qcond:String,qrowcond:String,qcolcond:String,
                           qchoicecond:String,qwhere:String )

  case class RDDXMLOutput2 (row:String,col:String,label:String,rowTitle:String,colTitle:String,title:String,values_title:String,values_value:String,altRowTitle:String,
                            altColTitle:String,rowcond:String,colcond :String,rowwhere:String,colwhere:String,qgroupby:String,open:String,value:String  )*/
    case class XMLParserOutput (){
      // Global Variables
      var version = ""
      var remerged = ""
      var surveyid = ""
      // question variables
      var fieldtype = ""
      var qlabel = ""
      var qtitle = ""
      var qalttitle = ""
      var qcond = ""
      var qrowcond = ""
      var qcolcond = ""
      var qchoicecond = ""
      var qwhere = ""

      var row = ""
      var col= ""
      var label = ""

      var rowTitle = ""
      var colTitle= ""
      var title = ""
      var values_title = ""
      var values_value = ""

      var altRowTitle = ""
      var altColTitle = ""


      var rowcond = ""
      var colcond = ""

      var rowwhere = ""
      var colwhere = ""

      var qgroupby = ""

      var open = ""
      var value = ""
      var rowindex = "0"
      var colindex = "0"

      var rootnode = ""

      var value_label = ""

      var looprows = ""
      var surveyname = ""
    }

  case class audit_logInfo(lastprocessdate:String,
                            filePath:String,
                            prev_vno:String,
                            new_vno:String,
                            surveryid: String)

  case class xmlparse_logInfo(log_date:String,
                           raw_filePath:String,
                           parse_xmlfilePath:String,
                           survey_id:String,
                           version_no:String)

  case class survey_names_master(survey_friendly_name: String,survey_id:String)

  case class customDataMapException(msg:String)  extends Exception

  case class datamapforlatest( surveyid:String,surveyname:String,remerged:String,version:String,
                               label:String,qtitle: String, qalttitle: String, rowTitle: String, colTitle: String, title: String)
}
