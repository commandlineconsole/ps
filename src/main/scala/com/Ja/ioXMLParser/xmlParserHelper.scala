package com.Ja.ioXMLParser

import scala.xml.NodeSeq

/**
  * Created by RCH28 on 05/04/2017.
  */
object xmlParserHelper {
    def getAttributeValue(node: NodeSeq, attributeName : String) : String = {

        if(!node.nonEmpty) "-"

        if(node.head.attribute(attributeName).nonEmpty)
          node.head.attribute(attributeName).toString()
        else
          "-"
    }
}
