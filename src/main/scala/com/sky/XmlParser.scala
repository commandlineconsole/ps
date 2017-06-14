package com.sky
import com.sky.domain.{XMLParserOutput, xmlParserLog}
import com.sky.ioXMLParser.xmlExtensions._
import com.typesafe.config.Config

import scala.collection.immutable.HashMap
import scala.collection.mutable.ArrayBuffer
import scala.xml.NodeSeq
/**
  *
  * Created by zhenhao.li on 29/11/2016.
  */


class XmlParser (config:Config, xmlString: String, surveyid:String, surveyName: String) {
  val html_regex = "<[^>]*>".r
  var datamap_date = ""
  var surveyVersion = ""
  val listOutput = new ArrayBuffer[XMLParserOutput]()

  val loopOutput = new ArrayBuffer[XMLParserOutput]()
  var raw = <none></none>
  val controlType_oe = "text"

  val controls_to_parse = List("radio","select" ,"checkbox", "textarea", "number", "float", "text")

  def parseForDumpTest(): StringBuilder =
  {
    raw = xml.XML.loadString(xmlString.toString())
    datamap_date = (raw \ "@remerged").text
    surveyVersion = "v" + (raw \ "@version").text
    val sb = StringBuilder.newBuilder


      blockParser()
      loopOnlyParser()
      outsideControlParser()
     val output = <records>
       {listOutput.map(x => caseClasstoXML(x))}
     </records>


    sb.append(output.mkString)


    sb
  }




  def parseForDump(): StringBuilder =
  {
    raw = xml.XML.loadString(xmlString.toString())
    datamap_date = (raw \ "@remerged").text
    surveyVersion = "v" + (raw \ "@version").text
    val sb = StringBuilder.newBuilder

    val logProcessor = new xmlParserLog(config)
    if(logProcessor.isFileAlreadyProcessed(surveyid, surveyVersion))
    {
      println("isFileAlreadyProcessed true : " + surveyVersion + " -  " + surveyid)
      //if file processed then exit with
      sb.append("<Error>FileAlreadyProcessed</Error>")
    }
    else {
      //Process different areas in the XML
      println("isFileAlreadyProcessed false : " + surveyVersion + " -  " + surveyid)
      blockParser()
      loopOnlyParser()
      outsideControlParser()

      //listOutput

      //return string as XML
      val output = <records>
        {listOutput.map(x => caseClasstoXML(x))}
      </records>


      sb.append(output.mkString)
    }

    logProcessor.hm.fs.close()

    sb
  }

  def replaceVariables(loopMap: HashMap[String, (String, String)], originalString: String): String = {
    var retString = originalString
    for (item <- loopMap) {
      if (originalString.contains("[loopvar: " + item._2._1 + "]")) {
        retString = retString.replace("[loopvar: " + item._2._1 + "]", item._2._2)
      }
    }
    retString
  }

  def caseClasstoXML(a: XMLParserOutput): NodeSeq = {

    //.replaceAll("\\s{2,}", " ").trim
    val x = <record>
      <version>{if(a.version == "v") "v1" else a.version }</version>
      <remerged>{a.remerged}</remerged>
      <surveyid>{a.surveyid}</surveyid>
      <surveyname>{a.surveyname}</surveyname>
      <type>{a.fieldtype}</type>
      <qlabel>{html_regex.replaceAllIn(a.qlabel, "").filter(_ >= ' ').replaceAll("\\s{2,}", " ").trim}</qlabel>
      <qtitle>{html_regex.replaceAllIn(a.qtitle, "").filter(_ >= ' ').replaceAll("\\s{2,}", " ").trim}</qtitle>
      <qalttitle>{html_regex.replaceAllIn(a.qalttitle, "").filter(_ >= ' ').replaceAll("\\s{2,}", " ").trim}</qalttitle>
      <qcond>{a.qcond.replaceAll("\\s{2,}", " ").trim}</qcond>
      <qrowcond>{a.qrowcond.replaceAll("\\s{2,}", " ").trim}</qrowcond>
      <qcolcond>{a.qcolcond.replaceAll("\\s{2,}", " ").trim}</qcolcond>
      <qchoicecond>{a.qchoicecond.replaceAll("\\s{2,}", " ").trim}</qchoicecond>
      <qwhere>{a.qwhere.replaceAll("\\s{2,}", " ").trim}</qwhere>
      <row>{html_regex.replaceAllIn(a.row, "").filter(_ >= ' ').replaceAll("\\s{2,}", " ").trim}</row>
      <col>{html_regex.replaceAllIn(a.col, "").filter(_ >= ' ')}</col>
      <label>{a.label}</label>
      <rowTitle>{html_regex.replaceAllIn(a.rowTitle, "").filter(_ >= ' ').replaceAll("\\s{2,}", " ").trim}</rowTitle>
      <colTitle>{html_regex.replaceAllIn(a.colTitle, "").filter(_ >= ' ').replaceAll("\\s{2,}", " ").trim}</colTitle>
      <title>{html_regex.replaceAllIn(a.title, "").filter(_ >= ' ').replaceAll("\\s{2,}", " ").trim}</title>
      <values_title>{html_regex.replaceAllIn(a.values_title, "").filter(_ >= ' ').replaceAll("\\s{2,}", " ").trim}</values_title>
      <values_value>{html_regex.replaceAllIn(a.values_value, "").filter(_ >= ' ').replaceAll("\\s{2,}", " ").trim}</values_value>
      <rowcond>{a.rowcond.replaceAll("\\s{2,}", " ").trim}</rowcond>
      <colcond>{a.colcond.replaceAll("\\s{2,}", " ").trim}</colcond>
      <rowwhere>{a.rowwhere.replaceAll("\\s{2,}", " ").trim}</rowwhere>
      <colwhere>{a.colwhere}</colwhere>
      <qgroupby>{a.qgroupby}</qgroupby>
      <open>{a.open}</open>
      <value>{a.value}</value>
      <values_label>{a.value_label}</values_label>
    </record>


    x
  }

  /*<altRowTitle>{html_regex.replaceAllIn(a.altRowTitle, "").replace("\r\n", "")}</altRowTitle>
    <altColTitle>{html_regex.replaceAllIn(a.altColTitle, "").replace("\r\n", "")}</altColTitle>
    <looprows>{a.looprows}</looprows>*/


  def getTitle(colTitle: String, rowTitle: String, qTitle: String): String = {
    var retvalue = ""
    if (!(colTitle.isEmpty || colTitle == "-"))
      retvalue = colTitle + " - "
    if (!(rowTitle.isEmpty || rowTitle == "-"))
      retvalue += rowTitle + " - "

    retvalue += qTitle
    retvalue
  }

  def loopOnlyParser() = {
    var rows = 0
    datamap_date = (raw \ "@remerged").text
    surveyVersion = "v" + (raw \ "@version").text
    var loopNodes = raw \\ "loop"

    for (node <- loopNodes) {
      var c = node.head
      val loopvar = c \\ "looprow"
      var loopIt = loopvar.iterator
      rows = loopvar.length

      var loopMap = new HashMap[String, (String, String)]()
      while (loopIt.hasNext) {
        val loopR = loopIt.next()
        val qLoopLabel = loopR.label
        val loopvarR = loopR \ "loopvar"
        for (i <- 0 to loopvarR.length - 1) {
          loopMap += ((loopR.attribute("label").get.toString() + "-" + loopvarR(i).attribute("name").get.toString()) -> (loopvarR(i).attribute("name").get.toString(), loopvarR(i).text.trim().replaceAll("\\s{2,}", " ")))
        }
      }

      loopOutput.clear()
      controls_to_parse.foreach( control => {
        chooseParser((c \\ control).filterByNotDp, "loop")
      })

      // Update the loop variables
      for (index <- 0 to loopOutput.length - 1) {
        var currentitem = getNewObject(loopOutput(index))
        var loopitem = getNewObject(currentitem)

        var loop = loopvar.iterator
        while (loop.hasNext) {

          var m = loop.next()

          var mappedVariables = loopMap.filter(m.attribute("label").get.toString() == _._1.split("-")(0))
          var loopL = m.attribute("label").get.toString()
          loopitem.qlabel = currentitem.qlabel.replace("[loopvar: label]", loopL)
          loopitem.label = currentitem.label.replace("[loopvar: label]", loopL)
          loopitem.rowTitle = currentitem.rowTitle.replace("[loopvar: label]", loopL)
          loopitem.qtitle = replaceVariables(mappedVariables, currentitem.qtitle)
          loopitem.title = replaceVariables(mappedVariables, currentitem.title)
          loopitem.rowTitle = replaceVariables(mappedVariables, currentitem.rowTitle)

          if(!loopitem.looprows.isEmpty)
            {
              if(loopitem.looprows.split(",").contains(loopL))
                listOutput += getNewObject(loopitem)
            }
          else
          {
            listOutput += getNewObject(loopitem)
          }
        }
      }
    }
  }


  def blockParser() = {

    //get loop blocks only
    var loopNodes = raw \ "block"
    for (node <- loopNodes) {
      var c = node.head
      try {
        controls_to_parse.foreach( control => {
          chooseParser((c \\ control).removeLoopNodes.filterByNotDp, "block")
        }
        )
      }
      catch{
        case e: Exception => {
          println("Exception in blockparser : " + e.getMessage)
        }
      }
    }
  }

  def outsideControlParser() = {
    datamap_date = (raw \ "@remerged").text
    surveyVersion = "v" + (raw \ "@version").text
    controls_to_parse.foreach( control => {
      chooseParser((raw \ control).removeLoopNodes.filterByNotDp, "out")
    })
  }

  def getQuestionDetails(questionNode:NodeSeq) = {
    val item = XMLParserOutput()
    item.qlabel = questionNode.attrValue("label")
    item.qcond = questionNode.attrValue("cond")
    item.qrowcond = questionNode.attrValue("rowCond")
    item.qcolcond = questionNode.attrValue("colCond")
    item.qchoicecond = questionNode.attrValue("choiceCond")
    item.qwhere = questionNode.attrValue("where")
    item.qgroupby = questionNode.attrValue("grouping")
    item.qtitle = questionNode.getNodeText("title")
    item.qalttitle = questionNode.getNodeText("alt")
    item.open = questionNode.attrValue("choiceCond")
    item.version = surveyVersion
    item.remerged = datamap_date
    item.surveyid = surveyid
    item.surveyname =surveyName
    item
  }

  def getChoiceDetails(cols:NodeSeq, q : XMLParserOutput ) = {
    val items = new ArrayBuffer[XMLParserOutput]
    if(cols.nonEmpty) {
      for (rw <- 0 to cols.length - 1) {
        var item = XMLParserOutput()
        //Update question details
        item = copydetails(q, item, "ques")
        //Retrieving data. Wait a few seconds and try to cut or copy again.
        item.colTitle = cols(rw).text.trim()
        item.altColTitle = cols(rw).attrValue("alt")
        item.col = cols(rw).attrValue("label")
        item.colwhere = cols(rw).attrValue("where")
        item.colcond = cols(rw).attrValue("cond")
        item.value = cols(rw).attrValue("value")
        item.value_label = cols(rw).attrValue("label")
        item.colindex = (rw + 1).toString
        items += getNewObject(item)
      }
    }

    items
  }

  def getColDetails(cols:NodeSeq, q : XMLParserOutput ) = {
    val items = new ArrayBuffer[XMLParserOutput]
    if(cols.nonEmpty) {
      for (rw <- 0 to cols.length - 1) {
        var item = XMLParserOutput()
        //Update question details
        item = copydetails(q, item, "ques")

        item.colTitle = cols(rw).text.trim()
        item.altColTitle = cols(rw).attrValue("alt")

        if(!item.altColTitle.isEmpty)
          item.colTitle = item.altColTitle

        item.col = cols(rw).attrValue("label")
        item.colwhere = cols(rw).attrValue("where")
        item.colcond = cols(rw).attrValue("cond")
        item.value = cols(rw).attrValue("value")
        item.value_label = cols(rw).attrValue("label")
        item.colindex = (rw + 1).toString
        items += item
      }
    }

    items
  }


  def getRowDetails(rows:NodeSeq, q : XMLParserOutput) = {
    val items = new ArrayBuffer[XMLParserOutput]
    if(rows.nonEmpty) {
      for (rw <- 0 to rows.length - 1) {
        var item = XMLParserOutput()
        //Update question details
        item = copydetails(q, item, "ques")
        item.rowTitle = rows(rw).text.trim()
        item.altRowTitle = rows(rw).attrValue("alt")
        if(!item.altRowTitle.isEmpty)
          item.rowTitle = item.altRowTitle
        item.row = rows(rw).attrValue("label")
        item.rowcond = rows(rw).attrValue("cond")
        item.rowwhere = rows(rw).attrValue("where")
        item.value = rows(rw).attrValue("value")
        item.value_label = rows(rw).attrValue("label")
        item.looprows = rows(rw).attrValue("looprows")
        item.rowindex = (rw + 1).toString
        items += item
      }
    }
    items
  }


  def processOpenItems(qNode:NodeSeq, sourceXML: String): Unit ={
    var qdetails = getQuestionDetails(qNode)
    // println(s"open question - rootnode - $sourceXML, quest : " + qdetails.qlabel)
    qdetails.fieldtype = controlType_oe
    qdetails.rootnode = sourceXML
    var rowdetails = getRowDetails((qNode \\ "row").filterByNotDp.filter(x => x.attribute("open").nonEmpty), qdetails)
    var coldetails = getColDetails((qNode \\ "col").filterByNotDp.filter(x => x.attribute("open").nonEmpty), qdetails)
    //var choicedetails = getChoiceDetails((qNode \\ "choice").filterByNotDp.filter(x => x.attribute("open").nonEmpty), qdetails)


    // if any open rows or columns present then create one for each.
    if(rowdetails.length > 0) {
      for (rw <- 0 to rowdetails.length - 1) {
        var item = getNewObject(rowdetails(rw))
        item.colTitle = ""
        item.col = ""
        item.values_title = ""
        item.values_value = ""
        item.value_label = ""
        item.title = getTitle(item.colTitle, item.rowTitle, item.qtitle)
        item.label = item.qlabel.replace("-", "") + rowdetails(rw).row.replace("-", "") + item.col.replace("-", "") + "oe"

        if (!sourceXML.equals("loop"))
          listOutput += getNewObject(item)
        else
          loopOutput += getNewObject(item)
      }
    }
    if(coldetails.length > 0) {
      for (ch <- 0 to coldetails.length - 1) {
        var item = getNewObject(coldetails(ch))
        item.rowTitle = ""
        item.row = ""
        item.values_title = ""
        item.values_value = ""
        item.value_label = ""
        item.title = getTitle(item.colTitle, item.rowTitle, item.qtitle)
        item.label = item.qlabel.replace("-", "") + coldetails(ch).row.replace("-", "") + item.col.replace("-", "") + "oe"
        if (!sourceXML.equals("loop"))
          listOutput += getNewObject(item)
        else
          loopOutput += getNewObject(item)
      }
    }
  }


  def  getNewObjectHeader(): XMLParserOutput ={

    var newItem = XMLParserOutput()
    // Global Variables
    newItem.version = "version"
    newItem.remerged = "remerged"
    newItem.surveyid = "surveyid"
    newItem.surveyname = "surveyName"
    // question variables
    newItem.fieldtype = "fieldtype"
    newItem.qlabel = "qlabel"
    newItem.qtitle = "qtitle"
    newItem.qalttitle = "qalttitle"
    newItem.qcond = "qcond"
    newItem.qrowcond = "qrowcond"
    newItem.qcolcond = "qcolcond"
    newItem.qchoicecond = "qchoicecond"
    newItem.qwhere = "qwhere"


    newItem.row = "row"
    newItem.col= "col"
    newItem.label = "label"

    newItem.rowTitle = "rowTitle"
    newItem.colTitle= "colTitle"
    newItem.title = "title"
    newItem.values_title = "values_title"
    newItem.values_value = "values_value"

    newItem.altRowTitle = "altRowTitle"
    newItem.altColTitle = "altColTitle"


    newItem.rowcond = "rowcond"
    newItem.colcond = "colcond"

    newItem.rowwhere = "rowwhere"
    newItem.colwhere = "colwhere"

    newItem.qgroupby = "qgroupby"

    newItem.open = "open"
    newItem.value = "value"
    newItem.rowindex = "rowindex"
    newItem.colindex = "colindex"

    newItem.rootnode = "rootnode"
    newItem.value_label = "value_label"
    newItem.looprows = "looprows"

    newItem

  }

  def  getNewObject(obj: XMLParserOutput): XMLParserOutput ={

    var newItem = XMLParserOutput()
    // Global Variables
    newItem.version = obj.version
    newItem.remerged = obj.remerged
    newItem.surveyid = obj.surveyid
    newItem.surveyname = obj.surveyname
    //    loopitem.rowTitle = scala.xml.XML.loadString(loopitem.rowTitle).text
    //    loopitem.title = scala.xml.XML.loadString(loopitem.title).text
    //    loopitem.colTitle = scala.xml.XML.loadString(loopitem.colTitle).text
    //    loopitem.col = scala.xml.XML.loadString(loopitem.col).text
    //    loopitem.row = scala.xml.XML.loadString(loopitem.row).text
    //    loopitem.qtitle = scala.xml.XML.loadString(loopitem.qtitle).text
    //    loopitem.values_value = scala.xml.XML.loadString(loopitem.values_value).text
    //    loopitem.values_title = scala.xml.XML.loadString(loopitem.values_title).text
    // question variables
    newItem.fieldtype = obj.fieldtype
    newItem.qlabel = obj.qlabel

    newItem.qcond = obj.qcond
    newItem.qrowcond = obj.qrowcond
    newItem.qcolcond = obj.qcolcond
    newItem.qchoicecond = obj.qchoicecond
    newItem.qwhere = obj.qwhere


    newItem.qtitle = obj.qtitle
    newItem.qalttitle =obj.qalttitle
    newItem.row = obj.row
    newItem.col= obj.col
    newItem.rowTitle = obj.rowTitle
    newItem.colTitle= obj.colTitle
    newItem.title = obj.title
    newItem.values_title = obj.values_title
    newItem.values_value = obj.values_value
    newItem.altRowTitle = obj.altRowTitle
    newItem.altColTitle = obj.altColTitle
    newItem.value_label = obj.value_label

    /* newItem.qtitle = scala.xml.XML.loadString(obj.qtitle).text
     newItem.qalttitle =scala.xml.XML.loadString(obj.qalttitle).text
     newItem.row = scala.xml.XML.loadString(obj.row).text
     newItem.col= scala.xml.XML.loadString(obj.col).text
     newItem.rowTitle = scala.xml.XML.loadString(obj.rowTitle).text
     newItem.colTitle= scala.xml.XML.loadString(obj.colTitle).text
     newItem.title = scala.xml.XML.loadString(obj.title).text
     newItem.values_title = scala.xml.XML.loadString(obj.values_title).text
     newItem.values_value = scala.xml.XML.loadString(obj.values_value).text
     newItem.altRowTitle = scala.xml.XML.loadString(obj.altRowTitle).text
     newItem.altColTitle = scala.xml.XML.loadString(obj.altColTitle).text*/



    newItem.label = obj.label




    newItem.rowcond = obj.rowcond
    newItem.colcond = obj.colcond

    newItem.rowwhere = obj.rowwhere
    newItem.colwhere = obj.colwhere

    newItem.qgroupby = obj.qgroupby

    newItem.open = obj.open
    newItem.value = obj.value
    newItem.rowindex = obj.rowindex
    newItem.colindex = obj.colindex
    newItem.rootnode = obj.rootnode
    newItem.looprows = obj.looprows

    newItem

  }

  def chooseParser(questionNode:NodeSeq, sourceXML: String) = {
    processNodes(questionNode,sourceXML)
  }

  def processNodes(questionNode:NodeSeq, sourceXML: String) ={
    if(!questionNode.isEmpty) {
      val controlName = questionNode.head.label
      val nextQuestion = questionNode.iterator
      while (nextQuestion.hasNext) {
        var qNode = nextQuestion.next()
        var qdetails = getQuestionDetails(qNode)
        if(qdetails.qlabel.equals("SQ1"))
          {
            println("break")
          }
        // println(s"rootnode - $sourceXML, quest : " + qdetails.qlabel)
        qdetails.fieldtype = controlName
        qdetails.rootnode = sourceXML
        var rowdetails = getRowDetails((qNode \\ "row").filterByNotDp, qdetails)
        var coldetails = getColDetails((qNode \\ "col").filterByNotDp, qdetails)
        var choicedetails = getChoiceDetails((qNode \\ "choice").filterByNotDp, qdetails)
        // This covers textarea, float, text, checkbox
        if (rowdetails.length <= 0 && coldetails.length <= 0 && choicedetails.length <= 0) {
          val item = getNewObject(qdetails)
          item.title = getTitle(item.colTitle, item.rowTitle, item.qtitle)
          item.label = item.qlabel.replace("-", "")
          if(!sourceXML.equals("loop"))
            listOutput += getNewObject(item)
          else
            loopOutput += getNewObject(item)
        }
        else {
          if (controlName.equals("select")) {
            if (choicedetails.length > 0 && rowdetails.length <= 0) {
              for (ch <- 0 to choicedetails.length - 1) {
                var item = getNewObject(choicedetails(ch))
                item.row = ""
                item.col = ""
                item.label = item.qlabel.replace("-", "")
                val ctitle = item.colTitle
                item.values_title = ctitle
                item.colTitle = ""
                item.title = getTitle(item.colTitle, item.rowTitle, item.qtitle)
                item.values_value = if (!item.value.isEmpty) item.value.trim() else (ch + 1).toString
                //listOutput += getNewObject(item)
                if(!sourceXML.equals("loop"))
                  listOutput += getNewObject(item)
                else
                  loopOutput += getNewObject(item)
              }
            }
            else {
              for (rw <- 0 to rowdetails.length - 1) {
                for (ch <- 0 to choicedetails.length - 1) {
                  var item = getNewObject(choicedetails(ch))
                  //Update row values into choice object
                  item = copydetails(rowdetails(rw), item, "row")
                  item.col = ""
                  item.label = item.qlabel.replace("-", "") + rowdetails(rw).row.replace("-", "") // + item.col.replace("-", "")
                  item.values_title = item.colTitle
                  item.colTitle =""
                  item.title = getTitle(item.colTitle, item.rowTitle, item.qtitle)
                  item.values_value = if (!item.value.isEmpty) item.value.trim() else (ch + 1).toString

                  //listOutput += getNewObject(item)
                  if(!sourceXML.equals("loop"))
                    listOutput += getNewObject(item)
                  else
                    loopOutput += getNewObject(item)
                }
              }
            }
          }
          else if (controlName.equals("number")) {
            //if no rows and cols then it is covered in first if
            //if only rows are not empty
            if (coldetails.length <= 0 && rowdetails.length > 0) {
              for (rw <- 0 to rowdetails.length - 1) {
                var item = getNewObject(rowdetails(rw))
                item.title = getTitle(item.colTitle, item.rowTitle, item.qtitle)
                item.label = item.qlabel.replace("-", "") + rowdetails(rw).row.replace("-", "") + item.col.replace("-", "")
                item.values_title = ""
                item.values_value = ""
                item.value_label = ""
                //listOutput += getNewObject(item)
                if(!sourceXML.equals("loop"))
                  listOutput += getNewObject(item)
                else
                  loopOutput += getNewObject(item)
              }
            }
            else if (coldetails.length > 0 && rowdetails.length > 0) {
              for (col <- 0 to coldetails.length - 1) {
                for (rw <- 0 to rowdetails.length - 1) {
                  var item = getNewObject(rowdetails(rw))
                  //Update col values into row object
                  item = copydetails(coldetails(col), item, "col")
                  item.title = getTitle(item.colTitle, item.rowTitle, item.qtitle)
                  item.label = item.qlabel.replace("-", "") + rowdetails(rw).row.replace("-", "") + item.col.replace("-", "")
                  item.values_title = ""
                  item.values_value = ""
                  item.value_label = ""
                  //listOutput += getNewObject(item)
                  if(!sourceXML.equals("loop"))
                    listOutput += getNewObject(item)
                  else
                    loopOutput += getNewObject(item)
                }
              }
            }
          }
          else if (controlName.equals("checkbox")) {
            if (coldetails.length > 0 && rowdetails.length > 0) {
              for (col <- 0 to coldetails.length - 1) {
                var colitem = coldetails(col)
                for (rw <- 0 to rowdetails.length - 1) {
                  var item = getNewObject(rowdetails(rw))
                  //Update col values into row object
                  item = copydetails(colitem, item, "col")
                  item.title = getTitle(item.colTitle, item.rowTitle, item.qtitle)
                  item.label = item.qlabel.replace("-", "") + rowdetails(rw).row.replace("-", "") + item.col.replace("-", "")
                  item.values_title = ""
                  item.values_value = ""
                  item.value_label = ""
                  // listOutput += getNewObject(item)
                  if(!sourceXML.equals("loop"))
                    listOutput += getNewObject(item)
                  else
                    loopOutput += getNewObject(item)
                }
              }
            }
            else if (rowdetails.length > 0) {
              for (rw <- 0 to rowdetails.length - 1) {
                var item = getNewObject(rowdetails(rw))
                item.title = getTitle(item.colTitle, item.rowTitle, item.qtitle)
                item.label = item.qlabel.replace("-", "") + rowdetails(rw).row.replace("-", "") + item.col.replace("-", "")
                item.values_title = ""
                item.values_value = ""
                item.value_label = ""
                // listOutput += getNewObject(item)
                if(!sourceXML.equals("loop"))
                  listOutput += getNewObject(item)
                else
                  loopOutput += getNewObject(item)
              }
            }
          }
          else if (controlName.equals("radio")) {
            // loops defining factors
            qdetails.qgroupby = if ((qNode \\ "@grouping").isEmpty) "rows" else (qNode \\ "@grouping").text
            qdetails.qgroupby = if ((qNode \\ "col").isEmpty) "cols1" else qdetails.qgroupby

            // if rows are empty
            if (coldetails.length > 0 && rowdetails.length <= 0) {
              for (col <- 0 to coldetails.length - 1) {
                var item = getNewObject(coldetails(col))
                item.values_title = item.colTitle
                item.values_value = if (!item.value.isEmpty) item.value.trim() else (col + 1).toString
                item.col = ""
                item.colTitle = ""
                item.title = getTitle(item.colTitle, item.rowTitle, item.qtitle)
                item.label = item.qlabel.replace("-", "") + coldetails(col).row.replace("-", "") + item.col.replace("-", "")
                //listOutput += getNewObject(item)
                if(!sourceXML.equals("loop"))
                  listOutput += getNewObject(item)
                else
                  loopOutput += getNewObject(item)
              }
            }
            else if (qdetails.qgroupby.equals("cols1")) {
              for (rw <- 0 to rowdetails.length - 1) {
                var item = getNewObject(rowdetails(rw))
                val orgRowTitle = item.rowTitle
                item.values_title = if (item.altRowTitle.trim().isEmpty) orgRowTitle else item.altRowTitle
                item.rowTitle = ""
                item.title = getTitle(item.colTitle, item.rowTitle, item.qtitle)
                item.row = ""
                item.col = ""
                item.label = item.qlabel.replace("-", "") + item.row.replace("-", "") + item.col.replace("-", "")
                item.values_value = if (!item.value.isEmpty) item.value.trim() else (rw + 1).toString
                //listOutput += getNewObject(item)
                if(!sourceXML.equals("loop"))
                  listOutput += getNewObject(item)
                else
                  loopOutput += getNewObject(item)
              }
            }
            else if (qdetails.qgroupby.equals("rows")) {
              for (rw <- 0 to rowdetails.length - 1) {
                var rowitem = getNewObject(rowdetails(rw))
                for (col <- 0 to coldetails.length - 1) {
                  var item10 = getNewObject(coldetails(col))
                  //Update col values into row object
                  item10.rowTitle =  rowdetails(rw).rowTitle
                  item10.rowcond  =  rowdetails(rw).rowcond
                  item10.rowwhere   =  rowdetails(rw).rowwhere
                  item10.row  =  rowdetails(rw).row
                  item10.altRowTitle = rowdetails(rw).altRowTitle
                  item10.rowindex =  rowdetails(rw).rowindex
                  //item = copydetails(rowdetails(rw), item, "row")
                  item10.col = ""
                  item10.values_title = if (!item10.altColTitle.equals("")) item10.altColTitle else item10.colTitle
                  item10.colTitle = ""
                  item10.title = getTitle(item10.colTitle, item10.rowTitle, item10.qtitle)
                  item10.label = item10.qlabel.replace("-", "") + item10.row.replace("-", "") + item10.col.replace("-", "")
                  item10.values_value = if (!item10.value.equals("")) item10.value.trim() else (col + 1).toString


                  item10.looprows = rowdetails(rw).looprows

                  if(!sourceXML.equals("loop"))
                    listOutput += getNewObject(item10)
                  else
                    loopOutput += getNewObject(item10)
                }
              }
            }
            else if (qdetails.qgroupby.equals("cols")) {
              for (col <- 0 to coldetails.length - 1) {
                for (rw <- 0 to rowdetails.length - 1) {
                  var item = getNewObject(rowdetails(rw))
                  //Update col values into row object
                  item = copydetails(coldetails(col), item, "col")
                  item.values_title = if (item.altRowTitle.trim().equals("")) item.rowTitle else item.altRowTitle.trim()
                  item.row = ""
                  item.rowTitle = ""
                  item.title = getTitle(item.colTitle, item.rowTitle, item.qtitle)
                  item.label = item.qlabel.replace("-", "") + item.row.replace("-", "") + item.col.replace("-", "")
                  item.values_value = if (!item.value.equals("")) item.value.trim() else item.rowindex
                  //listOutput += getNewObject(item)
                  if(!sourceXML.equals("loop"))
                    listOutput += getNewObject(item)
                  else
                    loopOutput += getNewObject(item)
                }
              }
            }
          }
        }

        //process open items
        processOpenItems(qNode, sourceXML)
      }
    }
  }

  def copydetails(fromObject: XMLParserOutput, toObject:XMLParserOutput, updateType:String):XMLParserOutput = {
    var destination = toObject
    if(updateType.equals("row")){
      destination.rowTitle =  fromObject.rowTitle
      destination.rowcond  =  fromObject.rowcond
      destination.rowwhere   =  fromObject.rowwhere
      destination.row  =  fromObject.row
      destination.altRowTitle = fromObject.altRowTitle
      destination.rowindex =  fromObject.rowindex
      //destination.value_label = fromObject.value_label
      destination.looprows = fromObject.looprows
    }
    else if(updateType.equals("col")){
      destination.colTitle=  fromObject.colTitle
      destination.colcond  =  fromObject.colcond
      destination.col   =  fromObject.col
      destination.colwhere  =  fromObject.colwhere
      destination.altColTitle = fromObject.altColTitle
      destination.colindex = fromObject.colindex
     // destination.value_label = fromObject.value_label
    }
    else if(updateType.equals("ques")){
      destination.fieldtype = fromObject.fieldtype
      destination.qalttitle = fromObject.qalttitle
      destination.qtitle = fromObject.qtitle
      destination.qgroupby = fromObject.qgroupby
      destination.qwhere = fromObject.qwhere
      destination.qcond = fromObject.qcond
      destination.qrowcond = fromObject.qrowcond
      destination.qlabel = fromObject.qlabel
      destination.qcolcond = fromObject.qcolcond
      destination.qchoicecond  = fromObject.qchoicecond
      destination.rootnode = fromObject.rootnode
      destination.surveyid = fromObject.surveyid
      destination.surveyname = fromObject.surveyname
      destination.remerged = fromObject.remerged
      destination.version = fromObject.version
    }

    destination
  }
}
