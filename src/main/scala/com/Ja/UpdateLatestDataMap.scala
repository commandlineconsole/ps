package com.Ja
import com.Ja.domain.{XMLParserOutput, xmlParserLog}
import com.Ja.common.hdfsFileManager
import com.Ja.conf.{ETLConf, HDFSConfForFileSystem}
import com.Ja.domain.datamapforlatest
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.types.StringType

import scala.collection.JavaConversions._
import scala.collection.immutable.HashMap
import scala.xml.NodeSeq
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import com.Ja.ioXMLParser.xmlExtensions._

/**
  * Created by RCH28 on 09/05/2017.
  */
object UpdateLatestDataMap {


  /*def UpdateLatestMap(latestDatamapsFilePath: String) : Unit = {
    try {
      val groupbyLabel = listOutput.groupBy(x => x.label).toMap
      println("2")
      val finalMap = groupbyLabel.map(elem => {
        datamapforlatest(surveyid, surveyName, elem._2.head.remerged, surveyVersion,
          elem._1, elem._2.head.qtitle, elem._2.head.qalttitle, elem._2.head.rowTitle, elem._2.head.colTitle)
      }).toList

      val ds = ETLConf.sc.parallelize(finalMap)
      println("4")
      import ETLConf.hiveContext.implicits._
      //val hc = new HiveContext(ETLConf.sc)
      //import hc.implicits._
      //import hc.sql
      val dataFrame = ds.toDF("surveyid", "surveyname", "remerged", "version", "label", "qtitle", "qalttitle", "rowTitle", "colTitle")
      dataFrame.coalesce(1).write.mode(SaveMode.Overwrite).format("com.databricks.spark.avro").mode("append").save(latestDatamapsFilePath)
    }
    catch
      {
        case e: Exception => println(s"Error while saving the latest data map at $latestDatamapsFilePath ---" + e.getMessage)
      }
  }*/




  def getMaxVersion(filepaths:List[String]) : (Int, String) = {

    //ab_datamap_bsb14022_v26.xml


    val ret = filepaths.map(filepath =>  (filepath.substring(filepath.lastIndexOf("_") +1 , filepath.lastIndexOf(".")).replace("v", "").toInt, filepath)).toList.maxBy(x => x._1)
    ret
  }

  def getQuestionDetails(raw:NodeSeq) : List[datamapforlatest] = {
    val node = raw \ "record"
    var lstrecords = ListBuffer[datamapforlatest]()
    for(elm <- node){
      lstrecords += datamapforlatest(node.getNodeText("surveyid"), node.getNodeText("surveyname"), node.getNodeText("remerged"), node.getNodeText("version"), node.getNodeText("label"), node.getNodeText("qtitle"), node.getNodeText("qalttitle"),
        node.getNodeText("rowTitle"), node.getNodeText("colTitle"), node.getNodeText("title"))
    }
    lstrecords.toList.distinct
  }
  def main(args: Array[String]): Unit = {
    val config: Config = ConfigFactory.load("application")
    val fsConfig = HDFSConfForFileSystem.getConfForHDFSFileSystem(config)
    val hm = new hdfsFileManager(fsConfig)
    val publishedXMLBaseOutputPath = config.getString("app.directory.hdfsPublishedParsedXMLOutputPath")
    val hdfsLatestDataMapsOutputFolderPath =  "/tmp/dmtest/"                   //config.getString("app.directory.hdfsLatestDataMapsOutputFolderPath")
    val hdfsLatestDataMapsOutputFileFormat = "lb_latestdatamap_%s"
    val surveys_to_process = config.getStringList("app.surveyidsForXMLParsing.names").toList
    if (surveys_to_process.length > 0) {
      val filesToProcess = hm.getParsedSurveyXMLFilePathsRecursively(publishedXMLBaseOutputPath, surveys_to_process)

     val latestmaps =  filesToProcess.map( x =>
        {
          if(!x._2.isEmpty) {
            val latestitem = getMaxVersion(x._2)
            (x._1, latestitem._1, latestitem._2)
          }
          else {
            (x._1, "", "")
          }
        }).toList

      latestmaps.foreach( x => {
        if(!x._3.isEmpty) {
          println("%s,%s".format(x._1, x._3))
          val latestfilepath = hdfsLatestDataMapsOutputFolderPath + hdfsLatestDataMapsOutputFileFormat.format(x._1)
          println("output file path:=" + latestfilepath)
          val datamapPrius_src = ETLConf.hiveContext.read.format("com.databricks.spark.xml").option("rowTag", "record").load(x._3)
          //val datamapPrius_src = datamapPrius_src1.repartition(100)
          /*val xmlString = hm.readHdfsFile(latestfilepath)
          val raw = xml.XML.loadString(xmlString.toString())
          val lstrecords = getQuestionDetails(raw)*/

          val datamapPrius_srcRdd = datamapPrius_src.repartition(5)
          import ETLConf.hiveContext.implicits._
         // val entries =datamapPrius_srcRdd.toDF




          val entries = datamapPrius_src.select(datamapPrius_src("surveyid"), datamapPrius_src("surveyname"), datamapPrius_src("remerged"), datamapPrius_src("version"), datamapPrius_src("label"), datamapPrius_src("qtitle"), datamapPrius_src("qalttitle"),
            datamapPrius_src("rowTitle"), datamapPrius_src("colTitle"), datamapPrius_src("title")).distinct().toDF()

          println("start...writing...")
          entries.repartition(5)
          println("****************************df count= " + entries.count())
          import com.databricks.spark.avro._
          entries.coalesce(5).write.format("com.databricks.spark.avro").save(latestfilepath)
          println("savecomplete")
        }
      })
    }
  }
}
