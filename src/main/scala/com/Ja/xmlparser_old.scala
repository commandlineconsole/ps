package com.Ja
import com.Ja.domain.XMLParserOutput
import java.io.{BufferedWriter, File, FileWriter}

import scala.collection.immutable.HashMap
import scala.collection.mutable.StringBuilder
import scala.collection.mutable.ListBuffer
import scala.xml.NodeSeq
import scala.collection.mutable.ArrayBuffer

/**
  * Created by zhenhao.li on 29/11/2016.
  */
object xmlparser_old {
  var datamap_date = ""
  var surveyVersion = ""
  val list = new ArrayBuffer[XMLParserOutput]()

  def main(args: Array[String]): Unit = {

    /** This part is to test radioParser */
    val testInput = xml.XML.loadFile("pirus.xml")
    val bw = new BufferedWriter(new FileWriter(new File("response_xml_parser.txt")))
    val header = "remerged" + "|" + "datamap_date" + "|" + "type" + "|" + "qlabel" + "|" + "rowlabel" + "|" + "collabel" + "|" + "label" + "|" + "qtitle" + "|" + "rowtitle" + "|" + "coltitle" + "|" + "title" + "|" + "values_title" + "|" + "values_value" + "|" + "altTitle" + "|" + "altRowTitle" + "|" + "altColTitle" + "\n"
    bw.write(header)

    blockParser(testInput, bw)
    loopOnlyParser(testInput, bw)
    outsideControlParser(testInput, bw)
    bw.close

  }

  /**
    *
    * Take in a raw XML object and return (question_Label, row)
    *
    **/

  def writeToBinary(questions: List[String], bw: BufferedWriter): Unit = {

    val questions_before = questions
    var questions_after = List[String]()
    for (index <- 0 to questions_before.length - 1) {
      var qA = questions_before(index).split("\\|")
      // qlabel,rowlabel,collabel, label, qtitle,rowtitle,coltitle,title,controltype, value_title, value_value

      val qlabel = if (qA(0).equals("-")) "" else qA(0)
      val row = if (qA(1).equals("-")) "" else qA(1)
      val col = if (qA(2).equals("-")) "" else qA(2)
      val label = if (qA(3).equals("-")) "" else qA(3)
      val qtitle = if (qA(4).equals("-")) "" else qA(4)
      val rowtitle = if (qA(5).equals("-")) "" else qA(5)
      val coltitle = if (qA(6).equals("-")) "" else qA(6)
      val title = if (qA(7).equals("-")) "" else qA(7)
      val controltype = if (qA(8).equals("-")) "" else qA(8)
      val value_title = if (qA(9).equals("-")) "" else qA(9)
      val value_value = if (qA(10).equals("-")) "" else qA(10)
      val altTitle = if (qA(11).equals("-")) "" else qA(11)
      val altRowTitle = if (qA(12).equals("-")) "" else qA(12)
      val altColTitle = if (qA(13).equals("-")) "" else qA(13)

      questions_after :+= surveyVersion + "|" + datamap_date + "|" + controltype + "|" + qlabel + "|" + row + "|" + col + "|" + label + "|" + qtitle + "|" + rowtitle + "|" + coltitle + "|" + title + "|" + value_title + "|" + value_value + "|" + altTitle + "|" + altRowTitle + "|" + altColTitle + "\n"

    }

    for (i <- 0 to questions_after.length - 1)
      bw.write(questions_after(i))

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

  def getTitle(colTitle: String, rowTitle: String, qTitle: String): String = {
    var retvalue = ""
    if (!(colTitle.isEmpty || colTitle == "-"))
      retvalue = colTitle + " - "
    if (!(rowTitle.isEmpty || rowTitle == "-"))
      retvalue += rowTitle + " - "

    retvalue += qTitle

    retvalue
  }

  def addRowToFile(qlabel:String, row:String, col:String, label:String, qtitle:String, rowtitle:String, coltitle:String, title:String, controltype:String, value_title:String, value_value:String, altTitle:String, altRowTitle:String, altColTitle:String, bw: BufferedWriter): Unit =
  {
    val qlabel1 =  if (qlabel.equals("-")) "" else qlabel
    val row1 = if (row.equals("-")) "" else row
    val col1 = if (col.equals("-")) "" else col
    val label1 = if (label.equals("-")) "" else label
    val qtitle1 = if (qtitle.equals("-")) "" else qtitle
    val rowtitle1 = if (rowtitle.equals("-")) "" else rowtitle
    val coltitle1 = if (coltitle.equals("-")) "" else coltitle
    val title1 = if (title.equals("-")) "" else title
    val controltype1 = if (controltype.equals("-")) "" else controltype
    val value_title1 = if (value_title.equals("-")) "" else value_title
    val value_value1 = if (value_value.equals("-")) "" else value_value
    val altTitle1 = if (altTitle.equals("-")) "" else altTitle
    val altRowTitle1 = if (altRowTitle.equals("-")) "" else altRowTitle
    val altColTitle1 = if (altColTitle.equals("-")) "" else altColTitle

    val finalString = surveyVersion + "|" + datamap_date + "|" + controltype1 + "|" + qlabel1 + "|" + row1 + "|" + col1 + "|" + label1 + "|" + qtitle1 + "|" + rowtitle1 + "|" + coltitle1 + "|" + title1 + "|" + value_title1 + "|" + value_value1 + "|" + altTitle1 + "|" + altRowTitle1 + "|" + altColTitle1 + "\n"
    bw.write(finalString)
  }

  def loopOnlyParser(raw: xml.Elem, bw: BufferedWriter) = {
    datamap_date = (raw \\ "@remerged").text
    surveyVersion = "v" + (raw \\ "@version").text
    //get loop blocks only
    var rows = 0
    var loopNodes = raw \\ "loop"

    for (node <- loopNodes) {
      var c = node.head
      var questions_after = List[String]()
      var questions_before = List[String]()
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

      questions_before = chooseParser(c \\ "radio", questions_before)
      questions_before = chooseParser(c \\ "select", questions_before)
      questions_before = chooseParser(c \\ "checkbox", questions_before)
      questions_before = chooseParser(c \\ "textarea", questions_before)
      questions_before = chooseParser(c \\ "number", questions_before)
      //loopOutput
      for (index <- 0 to questions_before.length - 1) {
        var loop = loopvar.iterator
        while (loop.hasNext) {
          var m = loop.next()
          var mappedVariables = loopMap.filter(m.attribute("label").get.toString() == _._1.split("-")(0))
          var loopL = m.attribute("label").get.toString()
          var qA = questions_before(index).split("\\|")
          // qlabel,rowlabel,collabel, label, qtitle,rowtitle,coltitle,title,controltype, value_title, value_value
          val qlabel = qA(0).replace("[loopvar: label]", loopL)
          val label = qA(3).replace("[loopvar: label]", loopL)
          val qtitle = replaceVariables(mappedVariables, qA(4))
          val title = replaceVariables(mappedVariables, qA(7))
          addRowToFile(qlabel,qA(1),qA(2),label,qtitle,qA(5),qA(6),title,qA(8),qA(9),qA(10),qA(11),qA(12),qA(13), bw)

          //questions_after :+= surveyVersion + "|" + datamap_date + "|" + controltype + "|" + qlabel + "|" + row + "|" + col + "|" + label + "|" + qtitle + "|" + rowtitle + "|" + coltitle + "|" + title + "|" + value_title + "|" + value_value + "|" + altTitle + "|" + altRowTitle + "|" + altColTitle + "\n"

        }
      }


      for (i <- 0 to questions_after.length - 1)
        bw.write(questions_after(i))
    }
  }


  def outsideControlParser(raw: xml.Elem, bw: BufferedWriter) = {
    datamap_date = (raw \\ "@remerged").text
    surveyVersion = "v" + (raw \\ "@version").text
    //get loop blocks only
    var rows = 0

    var questions_after = List[String]()
    var questions_before = List[String]()

    questions_before = chooseParser(raw \ "radio", questions_before)
    questions_before = chooseParser(raw \ "select", questions_before)
    questions_before = chooseParser(raw \ "checkbox", questions_before)
    questions_before = chooseParser(raw \ "textarea", questions_before)
    questions_before = chooseParser(raw \ "number", questions_before)
    questions_before = chooseParser(raw \ "float", questions_before)

    for (index <- 0 to questions_before.length - 1) {
      var qA = questions_before(index).split("\\|")
      // qlabel,rowlabel,collabel, label, qtitle,rowtitle,coltitle,title,controltype, value_title, value_value
      val qlabel = if (qA(0).equals("-")) "" else qA(0)
      val row = if (qA(1).equals("-")) "" else qA(1)
      val col = if (qA(2).equals("-")) "" else qA(2)
      val label = if (qA(3).equals("-")) "" else qA(3)
      val qtitle = if (qA(4).equals("-")) "" else qA(4)
      val rowtitle = if (qA(5).equals("-")) "" else qA(5)
      val coltitle = if (qA(6).equals("-")) "" else qA(6)
      val title = if (qA(7).equals("-")) "" else qA(7)
      val controltype = if (qA(8).equals("-")) "" else qA(8)
      val value_title = if (qA(9).equals("-")) "" else qA(9)
      val value_value = if (qA(10).equals("-")) "" else qA(10)
      val altTitle = if (qA(11).equals("-")) "" else qA(11)
      val altRowTitle = if (qA(12).equals("-")) "" else qA(12)
      val altColTitle = if (qA(13).equals("-")) "" else qA(13)
      questions_after :+= surveyVersion + "|" + datamap_date + "|" + controltype + "|" + qlabel + "|" + row + "|" + col + "|" + label + "|" + qtitle + "|" + rowtitle + "|" + coltitle + "|" + title + "|" + value_title + "|" + value_value + "|" + altTitle
      "|" + altRowTitle + "|" + altColTitle + "\n"
    }

    for (i <- 0 to questions_after.length - 1)
      bw.write(questions_after(i))

  }

  def blockParser(raw: xml.Elem, bw: BufferedWriter) = {
    datamap_date = (raw \\ "@remerged").text
    surveyVersion = "v" + (raw \\ "@version").text
    //get loop blocks only
    var rows = 0
    var loopNodes = raw \ "block"

    for (node <- loopNodes) {
      var c = node.head
      println((c \ "@label").text)
      var questions_after = List[String]()
      var questions_before = List[String]()

      //.filter(node => node.attribute("title")
      questions_before = chooseParser((c \\ "radio").filter(node => !node.attribute("label").toString().contains("[loop")), questions_before)
      questions_before = chooseParser((c \\ "select").filter(node => !node.attribute("label").toString().contains("[loop")), questions_before)
      questions_before = chooseParser((c \\ "checkbox").filter(node => !node.attribute("label").toString().contains("[loop")), questions_before)
      questions_before = chooseParser((c \\ "textarea").filter(node => !node.attribute("label").toString().contains("[loop")), questions_before)
      questions_before = chooseParser((c \\ "number").filter(node => !node.attribute("label").toString().contains("[loop")), questions_before)

      questions_before = chooseParser((c \\ "float").filter(node => !node.attribute("label").toString().contains("[loop")), questions_before)

      for (index <- 0 to questions_before.length - 1) {
        var qA = questions_before(index).split("\\|")
        // qlabel,rowlabel,collabel, label, qtitle,rowtitle,coltitle,title,controltype, value_title, value_value
        val qlabel = if (qA(0).equals("-")) "" else qA(0)
        val row = if (qA(1).equals("-")) "" else qA(1)
        val col = if (qA(2).equals("-")) "" else qA(2)
        val label = if (qA(3).equals("-")) "" else qA(3)
        val qtitle = if (qA(4).equals("-")) "" else qA(4)
        val rowtitle = if (qA(5).equals("-")) "" else qA(5)
        val coltitle = if (qA(6).equals("-")) "" else qA(6)
        val title = if (qA(7).equals("-")) "" else qA(7)
        val controltype = if (qA(8).equals("-")) "" else qA(8)
        val value_title = if (qA(9).equals("-")) "" else qA(9)
        val value_value = if (qA(10).equals("-")) "" else qA(10)
        val altTitle = if (qA(11).equals("-")) "" else qA(11)
        val altRowTitle = if (qA(12).equals("-")) "" else qA(12)
        val altColTitle = if (qA(13).equals("-")) "" else qA(13)
        questions_after :+= surveyVersion + "|" + datamap_date + "|" + controltype + "|" + qlabel + "|" + row + "|" + col + "|" + label + "|" + qtitle + "|" + rowtitle + "|" + coltitle + "|" + title + "|" + value_title + "|" + value_value + "|" + altTitle
        "|" + altRowTitle + "|" + altColTitle + "\n"
      }

      for (i <- 0 to questions_after.length - 1)
        bw.write(questions_after(i))
    }
  }


  def chooseParser(rsSelect:NodeSeq,questions:List[String]) = {
    var questions_before = List[String]()


    if(!rsSelect.isEmpty) {
      val controlName = rsSelect.head.label
      if (controlName.equals("radio"))
        questions_before = radioParser(rsSelect, questions)
      else if (controlName.equals("select"))
        questions_before = selectParser(rsSelect, questions)
      else if (controlName.equals("textarea") || controlName.equals("float"))
        questions_before = textareaParser(rsSelect, questions)
      else if (controlName.equals("number"))
        questions_before = numberParser(rsSelect, questions)
      else if (controlName.equals("checkbox"))
        questions_before = checkboxParser(rsSelect, questions)

    }
    else
    {
      println("No element found!")
      questions_before = questions
    }

    questions_before
  }


  def checkboxParser(rsCheckboxes:NodeSeq,questions:List[String]) = {

    var questions_before = questions
    var lstresponses = List[XMLParserOutput]()
    val radioIT = rsCheckboxes.iterator
    while (radioIT.hasNext) {
      var qtitle = "-"
      var altTitleText = ""

      var rowlabel = "-"
      var collabel = "-"
      var radioR = radioIT.next()
      var qlabel = radioR.attribute("label").get.text
      var controlType = radioR.label
      var alt = radioR \\ "alt"
      var col = radioR \\ "col"
      var row = radioR \\ "row"
      qtitle = (radioR \\ "title").text.trim().replaceAll("\\s{2,}", " ")
      if (alt.nonEmpty)
        altTitleText = alt.text.trim().replaceAll("\\s{2,}", " ")
      if(col.nonEmpty) {
        for (colRw <- 0 to col.length - 1) {
          val collabel = if (col(colRw).attribute("label").nonEmpty) col(colRw).attribute("label").get.toString().trim() else "-"
          val altColText = if (col(colRw).attribute("alt").nonEmpty) col(colRw).attribute("alt").get.toString().trim() else ""
          val coltitle = col(colRw).text.trim().replaceAll("\\s{2,}", " ")
          for (rw <- 0 to row.length - 1) {
            var rowText = row(rw).text.trim()
            val rowtitle = rowText
            val rowlabel = if (row(rw).attribute("label").nonEmpty) row(rw).attribute("label").get.toString().trim() else "-"
            val label = qlabel.replace("-", "") + rowlabel.replace("-", "") + collabel.replace("-", "")
            val title = getTitle(coltitle, rowtitle, qtitle)
            val value_title = "-"
            val value_value = "-"
            val altRowText =  if (row(rw).attribute("alt").nonEmpty) row(rw).attribute("alt").get.toString().trim() else ""
            questions_before :+= qlabel + "|" + rowlabel + "|" + collabel + "|" + label + "|" + qtitle + "|" + rowtitle + "|" + coltitle + "|" + title + "|" + controlType + "|" + value_title + "|" + value_value + "|" + formatForCSV(altTitleText) + "|" + formatForCSV(altRowText) + "|" + formatForCSV(altColText)
          }
        }
      }
      else
      {
        val collabel = "-"
        val coltitle = "-"
        val altColText = "-"
        for (rw <- 0 to row.length - 1) {
          var rowText = row(rw).text.trim()
          val rowtitle = rowText
          val rowlabel = if (row(rw).attribute("label").nonEmpty) row(rw).attribute("label").get.toString().trim() else "-"
          val label = qlabel.replace("-", "") + rowlabel.replace("-", "") + collabel.replace("-", "")
          val title = getTitle(coltitle, rowtitle, qtitle) //coltitle.replace("-", "") + rowtitle.replace("-", "") + qtitle.replace("-", "")
          val value_title = "-"
          val value_value = "-"

          val altRowText =  if (row(rw).attribute("alt").nonEmpty) row(rw).attribute("alt").get.toString().trim() else "-"
          //questions_before :+= qlabel + "|" + titleTxt + "|" + row(rw).attribute("label").get + "|" + "-" + "|" + col(colRw).attribute("label").get + "|" + colText + "|" + colText + "|" + (rw + 1) + "|" + altTitleText + "|" + altRowText + "|" + altColText + "|" + controlType
          // qlabel,rowlabel,collabel, label, qtitle,rowtitle,coltitle,title,controltype, value_title, value_value
          questions_before :+= qlabel + "|" + rowlabel + "|" + collabel + "|" + label + "|" + qtitle + "|" + rowtitle + "|" + coltitle + "|" + title + "|" + controlType + "|" + value_title + "|" + value_value + "|" + formatForCSV(altTitleText) + "|" + formatForCSV(altRowText) + "|" + formatForCSV(altColText)
        }
      }
    }

    questions_before
  }

  def numberParser(rsNumber:NodeSeq,questions:List[String]) ={
    var questions_before = questions
    var lstresponses = List[XMLParserOutput]()
    val radioIT = rsNumber.iterator
    while (radioIT.hasNext) {
      var qtitle = "-"
      var altTitleText = ""
      var altRowText = ""
      var rowlabel = "-"
      var collabel = "-"
      var altColText = ""
      var number = radioIT.next()
      var qlabel = number.attribute("label").get.text
      var controlType = number.label
      var alt = number \\ "alt"
      var col = number \\ "col"
      var row = number \\ "row"
      qtitle = (number \\ "title").text.trim().replaceAll("\\s{2,}", " ")
      if (alt.nonEmpty)
        altTitleText = alt.text.trim().replaceAll("\\s{2,}", " ").toString()

      qtitle = if(!altTitleText.isEmpty) altTitleText else qtitle
      /*   <number label="MOD2Q10NEW">
           <title>And how likely are you to upgrade to a fibre broadband package, either with your current or another provider, within the next 12 months?</title>
         </number>


         */
      //When no rows or columns present
      if(row.isEmpty && col.isEmpty)
      {
        var rowText = "-"
        altRowText = ""
        rowlabel = "-"
        collabel = "-"
        val label = qlabel.replace("-", "") + rowlabel.replace("-", "") + collabel.replace("-", "")
        val rowtitle = "-"
        val coltitle = "-"
        val title = getTitle(coltitle, rowtitle, qtitle) //coltitle.replace("-", "") + rowtitle.replace("-", "") + qtitle.replace("-", "")
      val value_title = "-"
        val value_value = "-"
        val altColText = ""
        questions_before :+= qlabel + "|" + rowlabel + "|" + collabel + "|" + label + "|" + qtitle + "|" + rowtitle + "|" + coltitle + "|" + title + "|" + controlType + "|" + value_title + "|" + value_value + "|" + formatForCSV(altTitleText) + "|" + formatForCSV(altRowText) + "|" + formatForCSV(altColText)
      }
      else if(!col.nonEmpty) {
        for (rw <- 0 to row.length - 1) {
          var rowText = row(rw).text.trim().replaceAll("\\s{2,}", " ")
          altRowText = if (row(rw).attribute("alt").nonEmpty) row(rw).attribute("alt").get.toString().trim().replaceAll("\\s{2,}", " ") else ""
          rowlabel = if (row(rw).attribute("label").nonEmpty) row(rw).attribute("label").get.toString().trim() else "-"
          collabel = "-"
          val label = qlabel.replace("-", "") + rowlabel.replace("-", "") + collabel.replace("-", "")
          val rowtitle = if (altRowText.trim().isEmpty) rowText else altRowText
          val coltitle = "-"
          val title = getTitle(coltitle, rowtitle, qtitle) //coltitle.replace("-", "") + rowtitle.replace("-", "") + qtitle.replace("-", "")
          val value_title = "-"
          val value_value = "-"
          val altColText = ""
          questions_before :+= qlabel + "|" + rowlabel + "|" + collabel + "|" + label + "|" + qtitle + "|" + rowtitle + "|" + coltitle + "|" + title + "|" + controlType + "|" + value_title + "|" + value_value + "|" + formatForCSV(altTitleText) + "|" + formatForCSV(altRowText) + "|" + formatForCSV(altColText)
        }
      }
      else{
        for (colRw <- 0 to col.length - 1) {
          val collabel =  if (col(colRw).attribute("label").nonEmpty) col(colRw).attribute("label").get.toString().trim() else "-"
          altColText = if (col(colRw).attribute("alt").nonEmpty) col(colRw).attribute("alt").get.toString().trim().replaceAll("\\s{2,}", " ").trim() else ""
          val coltitle = if(!altColText.isEmpty) altColText else col(colRw).text.trim().replaceAll("\\s{2,}", " ")

          for (rw <- 0 to row.length - 1) {
            var rowText = row(rw).text.trim()
            altRowText = if (row(rw).attribute("alt").nonEmpty) row(rw).attribute("alt").get.toString().trim() else ""
            var rowtitle = if (altRowText.trim().isEmpty) rowText else altRowText
            //val rowtitle = "-"
            val rowlabel = if (row(rw).attribute("label").nonEmpty) row(rw).attribute("label").get.toString().trim() else "-"
            val label = qlabel.replace("-", "") + rowlabel.replace("-", "") + collabel.replace("-", "")
            val title = getTitle(coltitle, rowtitle, qtitle) //coltitle.replace("-", "") + rowtitle.replace("-", "") + qtitle.replace("-", "")
            val value_title = "-"
            val value_value = "-"
            questions_before :+= qlabel + "|" + rowlabel + "|" + collabel + "|" + label + "|" + qtitle + "|" + rowtitle + "|" + coltitle + "|" + title + "|" + controlType + "|" + value_title + "|" + value_value + "|" + formatForCSV(altTitleText) + "|" + formatForCSV(altRowText) + "|" + formatForCSV(altColText)
          }
        }
      }
    }
    questions_before
  }

  def textareaParser(rsTextArea:NodeSeq,questions:List[String]) = {

    /*
     <textarea label="MB4a" cond="MB4.r1" optional="0">
        <title>Can you tell us why you are actively looking to switch mobile provider?</title>
        <comment>Please give as much detail as you can.</comment>
    </textarea>
    <float label="qtimebeforeCQ12" where="report">
      <title>Total Interview Time Before CQ12</title>
  </float>
    Textarea. Details  to extract datamap from the xml given below.
    Text under title tag will become the qtitle and if they are any <alt> tag then text inside that tag will be qtitle
    Values and values_title will be empty.

     */

    var questions_before = questions
    val radioIT = rsTextArea.iterator
    while (radioIT.hasNext) {
      var textArea = radioIT.next()
      val qlabel = textArea.attribute("label").get.toString()  //Label to textarea will become qlablel and label
      if(qlabel.equals("qThanksNew"))
      {
        println("stop")
      }
      var qtitle =if (!(textArea \\ "title").isEmpty) (textArea \\ "title").text.trim() else "-"
      val controlType = textArea.label
      var rowtitle = "-"
      var rowlabel = "-"
      val collabel = "-"
      val label = qlabel.replace("-", "")
      val coltitle = "-"
      var title = getTitle(coltitle, rowtitle, qtitle) //coltitle.replace("-", "") + rowtitle.replace("-", "") + qtitle.replace("-", "")
      var value_title = "-"
      var value_value = "-"
      val altRowText = ""
      val altColText= ""
      val altTitleText = if (!(textArea \\ "alt").isEmpty) (textArea \\ "alt").text else ""
      questions_before :+= qlabel + "|" + rowlabel + "|" + collabel + "|" + label + "|" + qtitle + "|" + rowtitle + "|" + coltitle + "|" + title + "|" + controlType + "|" + value_title + "|" + value_value + "|" + formatForCSV(altTitleText) + "|" + formatForCSV(altRowText) + "|" + formatForCSV(altColText)
    }
    questions_before
  }
  def selectParser(rsSelect:NodeSeq,questions:List[String]) = {


    var questions_before = questions
    var rsSelectIT = rsSelect.iterator
    var controlType = "select"
    while(rsSelectIT.hasNext){
      var titleTxt = ""
      var altTitleText = ""
      var altRowText = ""
      var altColText = ""
      var selectR = rsSelectIT.next()

      var title = selectR \\ "title"
      var alt = selectR \\ "alt"
      var col = selectR \\ "col"
      var row = selectR \\ "row"
      var choice = selectR \\ "choice"
      titleTxt = title.text.trim().replaceAll("\\s{2,}", " ")
      if (alt.nonEmpty) altTitleText = alt.text.trim().replaceAll("\\s{2,}", " ") else ""
      var qtitle = if (alt.nonEmpty) alt.text.trim().replaceAll("\\s{2,}", " ") else titleTxt
      var qlabel = selectR.attribute("label").get.toString()

      if( row.nonEmpty ) {
        for (rw <- 0 to row.length - 1) {
          var rowtitle = row(rw).text.trim().replaceAll("\\s{2,}", " ")
          var rowlabel = if (row(rw).attribute("label").nonEmpty) row(rw).attribute("label").get.toString().trim() else "-"
          val collabel = "-"
          val label = qlabel.replace("-", "") + rowlabel.replace("-", "") + collabel.replace("-", "")
          val coltitle = "-"
          val altRowText =  if (row(rw).attribute("alt").nonEmpty) row(rw).attribute("alt").get.toString().trim() else ""

          for(i <- 0 to choice.length -1){
            var title = getTitle(coltitle, rowtitle, qtitle) // coltitle.replace("-", "") + rowtitle.replace("-", "") + qtitle.replace("-", "")
            var chTagTxt = choice(i).text.trim()
            var value_title = chTagTxt.replaceAll("\\s{2,}", ":").split(":")(0)
            var value_value = (i+1).toString()
            val altColText = if (choice(i).attribute("alt").nonEmpty) choice(i).attribute("alt").get.toString().trim() else ""


            // qlabel,rowlabel,collabel, label, qtitle,rowtitle,coltitle,title,controltype, value_title, value_value
            questions_before :+= qlabel + "|" + rowlabel + "|" + collabel + "|" + label + "|" + qtitle + "|" + rowtitle + "|" + coltitle + "|" + title + "|" + controlType + "|" + value_title + "|" + value_value + "|" + formatForCSV(altTitleText) + "|" + formatForCSV(altRowText) + "|" + formatForCSV(altColText)
          }
        }
      }
      else
      {
        var rowtitle = "-"
        var rowlabel = "-"
        val collabel = "-"
        val label = qlabel.replace("-", "") + rowlabel.replace("-", "") + collabel.replace("-", "")
        val coltitle = "-"

        for(i <- 0 to choice.length -1){
          var title = getTitle(coltitle, rowtitle, qtitle) //coltitle.replace("-", "") + rowtitle.replace("-", "") + qtitle.replace("-", "")
          var chTagTxt = choice(i).text
          var value_title = chTagTxt.replaceAll("\\s{2,}", ":").split(":")(0)
          var value_value = (i+1).toString()
          val altColText = if (choice(i).attribute("alt").nonEmpty) choice(i).attribute("alt").get.toString().trim() else "-"
          val altRowText =  "-"
          // qlabel,rowlabel,collabel, label, qtitle,rowtitle,coltitle,title,controltype, value_title, value_value
          questions_before :+= qlabel + "|" + rowlabel + "|" + collabel + "|" + label + "|" + qtitle + "|" + rowtitle + "|" + coltitle + "|" + title + "|" + controlType + "|" + value_title + "|" + value_value + "|" + formatForCSV(altTitleText) + "|" + formatForCSV(altRowText) + "|" + formatForCSV(altColText)
        }
      }
    }

    questions_before
  }

  def radioParser(rsRadio:NodeSeq,questions:List[String]) ={
    var questions_before = questions
    var lstresponses = List[XMLParserOutput]()
    val radioIT = rsRadio.iterator
    while (radioIT.hasNext) {
      var qtitle = "-"
      var altTitleText = ""
      var altRowText = ""
      var rowlabel = ""
      var collabel = "-"
      var altColText = "-"
      var radioR = radioIT.next()
      var qlabel = radioR.attribute("label").get.text
      var controlType = radioR.label
      var alt = radioR \\ "alt"
      var col = radioR \\ "col"
      var groupby =  if((radioR \\ "@grouping").isEmpty) "rows" else (radioR \\ "@grouping").text
      groupby = if((radioR \\ "col").isEmpty) "cols1" else groupby
      var row = radioR \\ "row"
      qtitle = (radioR \\ "title").text.trim().replaceAll("\\s{2,}", " ")
      if (alt.nonEmpty)
        altTitleText = alt.text.trim().replaceAll("\\s{2,}", " ")

      qtitle = if(!altTitleText.isEmpty) altTitleText else qtitle


      //When Only columns present then there is no grouping concept
      if(row.isEmpty)
      {
        for (cw <- 0 to col.length - 1) {
          var colText = col(cw).text.trim().replaceAll("\\s{2,}", " ")
          altColText = if (col(cw).attribute("alt").nonEmpty) col(cw).attribute("alt").get.toString().trim().replaceAll("\\s{2,}", " ") else ""
          rowlabel = "-" //if (row(rw).attribute("label").nonEmpty) row(rw).attribute("label").get.toString().trim() else "-"
          collabel = "-" //if (col(cw).attribute("label").nonEmpty) col(cw).attribute("label").get.toString().trim() else "-"
          val label = qlabel.replace("-", "") + rowlabel.replace("-", "") + collabel.replace("-", "")
          val rowtitle = ""
          val coltitle = "-"
          val title = getTitle(coltitle, rowtitle.replace("-", "") , qtitle.replace("-", ""))
          val value_title = if(!altColText.isEmpty) altColText else colText
          val value_value = (cw + 1)
          val altRowText =  "-"

          questions_before :+= qlabel + "|" + rowlabel + "|" + collabel + "|" + label + "|" + qtitle + "|" + rowtitle + "|" + coltitle + "|" + title + "|" + controlType + "|" + value_title + "|" + value_value+ "|" + formatForCSV(altTitleText) + "|" + formatForCSV(altRowText) + "|" + formatForCSV(altColText)
        }
      }

      if(groupby.equals("cols1")) {
        for (rw <- 0 to row.length - 1) {
          var rowText = row(rw).text.trim().replaceAll("\\s{2,}", " ")
          altRowText = if (row(rw).attribute("alt").nonEmpty) row(rw).attribute("alt").get.toString().trim().replaceAll("\\s{2,}", " ") else ""
          rowlabel = "-" //if (row(rw).attribute("label").nonEmpty) row(rw).attribute("label").get.toString().trim() else "-"
          collabel = "-"
          val label = qlabel.replace("-", "") + rowlabel.replace("-", "") + collabel.replace("-", "")
          val org_rowtitle = if (altRowText.trim().isEmpty) rowText else altRowText
          val rowtitle = org_rowtitle
          val coltitle = "-"
          val title = getTitle(coltitle, rowtitle.replace("-", "") , qtitle.replace("-", ""))
          val value_title = org_rowtitle
          val value_value = (rw + 1)

          val altColText = ""
          questions_before :+= qlabel + "|" + rowlabel + "|" + collabel + "|" + label + "|" + qtitle + "|" + rowtitle + "|" + coltitle + "|" + title + "|" + controlType + "|" + value_title + "|" + value_value + "|" + formatForCSV(altTitleText) + "|" + formatForCSV(altRowText) + "|" + formatForCSV(altColText)
        }
      }
      else if(groupby.equals("rows")) {
        for (rw <- 0 to row.length - 1) {
          var rowText = row(rw).text.trim().replaceAll("\\s{2,}", " ")
          altRowText = if (row(rw).attribute("alt").nonEmpty) row(rw).attribute("alt").get.toString().trim().replaceAll("\\s{2,}", " ") else ""
          var rowtitle = if (altRowText.trim().isEmpty) rowText else altRowText
          rowlabel = if (row(rw).attribute("label").nonEmpty) row(rw).attribute("label").get.toString().trim() else "-"

          for (colRw <- 0 to col.length - 1) {
            val collabel = "-"
            altColText = if (col(colRw).attribute("alt").nonEmpty) col(colRw).attribute("alt").get.toString().trim().replaceAll("\\s{2,}", " ").trim() else ""
            val org_coltitle = if(!altColText.isEmpty) altColText else col(colRw).text.trim().replaceAll("\\s{2,}", " ")
            val coltitle = "-" //if(altColText.isEmpty) altColText else col(colRw).text.trim().replaceAll("\\s{2,}", " ")
            val label = qlabel.replace("-", "") + rowlabel.replace("-", "") + collabel.replace("-", "")
            val title = getTitle(coltitle, rowtitle, qtitle) // coltitle.replace("-", "") + rowtitle.replace("-", "") + qtitle.replace("-", "")
            val value_title = org_coltitle
            val value_value = (colRw + 1)
            //questions_before :+= qlabel + "|" + titleTxt + "|" + row(rw).attribute("label").get + "|" + "-" + "|" + col(colRw).attribute("label").get + "|" + colText + "|" + colText + "|" + (rw + 1) + "|" + formatForCSV(altTitleText) + "|" + formatForCSV(altRowText) + "|" + formatForCSV(altColText) + "|" + controlType
            // qlabel,rowlabel,collabel, label, qtitle,rowtitle,coltitle,title,controltype, value_title, value_value
            questions_before :+= qlabel + "|" + rowlabel + "|" + collabel + "|" + label + "|" + qtitle + "|" + rowtitle + "|" + coltitle + "|" + title + "|" + controlType + "|" + value_title + "|" + value_value + "|" + formatForCSV(altTitleText) + "|" + formatForCSV(altRowText) + "|" + formatForCSV(altColText)
          }
        }
      }

      else if(groupby.equals("cols")) {
        for (colRw <- 0 to col.length - 1) {
          val collabel =  if (col(colRw).attribute("label").nonEmpty) col(colRw).attribute("label").get.toString().trim() else "-"
          altColText = if (col(colRw).attribute("alt").nonEmpty) col(colRw).attribute("alt").get.toString().trim().replaceAll("\\s{2,}", " ").trim() else ""
          val coltitle = if(!altColText.isEmpty) altColText else col(colRw).text.trim().replaceAll("\\s{2,}", " ")
          for (rw <- 0 to row.length - 1) {
            var rowText = row(rw).text.trim()
            altRowText = if (row(rw).attribute("alt").nonEmpty) row(rw).attribute("alt").get.toString().trim() else ""
            var org_rowtitle = if (altRowText.trim().isEmpty) rowText else altRowText
            val rowtitle = "-"
            val rowlabel = "-" //if (row(rw).attribute("label").nonEmpty) row(rw).attribute("label").get.toString().trim() else "-"
            val label = qlabel.replace("-", "") + rowlabel.replace("-", "") + collabel.replace("-", "")
            val title = getTitle(coltitle, rowtitle, qtitle) // coltitle.replace("-", "") + rowtitle.replace("-", "") + qtitle.replace("-", "")
            val value_title = org_rowtitle
            val value_value = (rw + 1)
            //questions_before :+= qlabel + "|" + titleTxt + "|" + row(rw).attribute("label").get + "|" + "-" + "|" + col(colRw).attribute("label").get + "|" + colText + "|" + colText + "|" + (rw + 1) + "|" + formatForCSV(altTitleText) + "|" + formatForCSV(altRowText) + "|" + formatForCSV(altColText) + "|" + controlType
            // qlabel,rowlabel,collabel, label, qtitle,rowtitle,coltitle,title,controltype, value_title, value_value
            questions_before :+= qlabel + "|" + rowlabel + "|" + collabel + "|" + label + "|" + qtitle + "|" + rowtitle + "|" + coltitle + "|" + title + "|" + controlType + "|" + value_title + "|" + value_value+ "|" + formatForCSV(altTitleText) + "|" + formatForCSV(altRowText) + "|" + formatForCSV(altColText)
          }
        }
      }
    }
    questions_before
  }

  def formatForCSV(item: String) : String = {
    var retvalue = "-"
    if(!item.equals(""))
      retvalue = item

    retvalue
  }

}

