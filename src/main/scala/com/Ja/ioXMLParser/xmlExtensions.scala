package com.Ja.ioXMLParser

import scala.xml.NodeSeq

/**
  * Created by RCH28 on 05/04/2017.
  */

object xmlExtensions
{
  implicit class XMLExtensions(raw:NodeSeq){

    def filterByNotDp = raw.filter(x => ((x.attribute("where").isEmpty) ||
      (!(x.attribute("where").
        get.toString().contains("notdp")))))

    def removeLoopNodes = {
      raw.filter(node => !node.attribute("label").toString().contains("[loop"))

    }
    def attrValue(attrName: String):String =  {
      if(!raw.nonEmpty) ""

      if(raw.head.attribute(attrName).nonEmpty)
        raw.head.attribute(attrName).get.toString()
      else
        ""
    }
    def getNodeText(nodeName: String):String =  {
      if(!raw.nonEmpty) ""
      if((raw \\ nodeName).nonEmpty) {
        val value = (raw \\ nodeName).text.trim
        value
      }
      else {
        ""
      }
    }
  }
}

