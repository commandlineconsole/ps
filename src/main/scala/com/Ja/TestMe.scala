
package com.Ja
import com.Ja.domain.{XMLParserOutput, audit_logInfo, customDataMapException}
import java.io.{BufferedWriter, File, FileWriter}
import java.util.Calendar

import com.Ja.common.hdfsFileManager
import com.Ja.conf.{ETLConf, HDFSConfForFileSystem}
import com.typesafe.config.{Config, ConfigFactory}

import scala.collection.immutable.HashMap
import scala.collection.mutable.StringBuilder
import scala.collection.mutable.ListBuffer
import scala.xml.NodeSeq
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._

/**
  * Created by zhenhao.li on 29/11/2016.
  */


import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SaveMode};
import org.apache.spark.sql.types.{StructType,StructField,StringType};


object TestMe {
  var datamap_date = ""
  var surveyVersion = ""
  val list = new ArrayBuffer[XMLParserOutput]()
  //import once, use everywhere
  implicit def date2timestamp(date: java.util.Date) =  new java.sql.Timestamp(date.getTime)
  val date = new java.util.Date
  //conversion happens implicitly
  val timestamp: java.sql.Timestamp = date


  def main(args: Array[String]): Unit = {

    val dat = new java.text.SimpleDateFormat("MM/dd/yyyy HH:mm").parse("12/07/2016 14:28")
    var cal: Calendar = Calendar.getInstance()
    cal.setTime(dat)
    val minuteFormat = new java.text.SimpleDateFormat("dd/MM/yyyy HH:mm").format(cal.getTime)
    println(minuteFormat)






//    val hc = new org.apache.spark.sql.hive.HiveContext(ETLConf.sc)
    // Queries are expressed in HiveQL
  //  hc.sql("SELECT uuid FROM pr.ab_response LIMIT 5").collect().foreach(println)

    /*val config: Config = ConfigFactory.load("application")
    /** This part is to test radioParser */
   /* val testInput = xml.XML.loadFile("pirus.xml")
    val parser = new XmlParser(config, testInput.toString, "xxxx")
    val contentToSave = parser.parseForDumpTest()

    val bw = new BufferedWriter(new FileWriter(new File("response_xml_parser.txt")))
    bw.write(contentToSave.mkString)
    bw.close()*/

    println("when version 22")
    println(getMatchingDataMapFile(config,"bsb14022",22))

    println("when version 26")
    println(getMatchingDataMapFile(config,"bsb14022",26))


    println("when version 55")
    println(getMatchingDataMapFile(config,"bsb14022",55))*/
  }
}
  /*def getMatchingDataMapFile(config: Config, surveyID : String, versionNo: Int) : String = {
    var ret = ""
    try {
      val fsConfig = HDFSConfForFileSystem.getConfForHDFSFileSystem(config)
      val hm = new hdfsFileManager(fsConfig)
      val surveys_to_process = List(surveyID)
      val surveys_root_path: String = s"/data/test/pr/published/atomicblocks/ab_datamap/$surveyID/" // config.getString("app.directory.hdfsSourceBasePath") + "/$surveyID/"
      var availableVersions = new HashMap[Int, String]()
      if (surveys_to_process.length > 0) {
        val filesToProcess = hm.getSurveyXMLPaths(surveys_root_path, "")
        //Sample file format ab_datamap_bsb14022_v26.xml
        filesToProcess.foreach(x => {
          val fileversion = x.substring(x.lastIndexOf("_") + 1, x.lastIndexOf(".")).toLowerCase.replace("v", "").toInt
          availableVersions += (fileversion -> x)
        })

        if (versionNo <= availableVersions.keys.min) {
          val minvalue = availableVersions.keys.min
          ret = availableVersions.filter(x => x._1 == minvalue).head._2
        }
        else {
          if (availableVersions.contains(versionNo)) {
            ret = availableVersions.filter(x => x._1 == versionNo).head._2
          }
          else {
            throw new Exception("Matching version not found in DataMap")
          }
        }


       // availableVersions.foreach(x => println("striped out version: " + x))
      }
      else {
        println("no entries in audit log")
      }
    }
    catch {
      case x: customDataMapException => throw new customDataMapException("Matching version not found in DataMap")
      case y : Exception => throw new Exception("Error in finding Matching DataMap file : " + y.getMessage)
    }

    ret
  }*/

