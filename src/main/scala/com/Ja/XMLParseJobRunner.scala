package com.Ja
import com.Ja.common.hdfsFileManager
import com.Ja.conf.{ETLConf, HDFSConfForFileSystem, JobConfig}
import com.Ja.domain.{customDataMapException, xmlParserLog, xmlparse_logInfo}
import com.typesafe.config.{Config, ConfigFactory}

import scala.collection.JavaConversions._
import scala.collection._
import scala.collection.mutable.ArrayBuffer

object XMLParseJobRunner
{

def main(args: Array[String]): Unit = {
  val config: Config = ConfigFactory.load("application")
  val fsConfig = HDFSConfForFileSystem.getConfForHDFSFileSystem(config)
  val hm = new hdfsFileManager(fsConfig)

  val stateXMLLogFilePath = config.getString("app.directory.hdfsSurveysXmlparserLogPath")
  val surveys_to_process = config.getStringList("app.surveyidsForXMLParsing.names").toList
  val surveys_root_path: String = config.getString("app.directory.hdfsSourceBasePath")
  val processedFilePath = config.getString("app.directory.hdfsXMLProcessedFilePath")

  val publishedXMLBaseOutputPathFormat = config.getString("app.directory.hdfsPublishedParsedXMLOutputPath") + "%s/" // "/data/published/pr/atomicblocks/ab_datamap/"
  try {

    //Check the master details file

    def getSurveyDescription(surveyid: String) : String = {
      val masterFilePath = config.getString("app.directory.hdfsSurveyNamesMasterFilePath")
      //survey_friendly_name,survey_id
      val surveyNames = hm.readSurveyNamesMaster(masterFilePath)
      val ret = surveyNames.filter(x => x.survey_id.trim.toLowerCase.equals(surveyid.toLowerCase))
      var retValue = ""
      if(!ret.isEmpty)
        retValue = ret.head.survey_friendly_name
      retValue
    }



    if (surveys_to_process.length > 0) {
      val filesToProcess = hm.getSurveyXMLFilePathsRecursively(surveys_root_path, surveys_to_process)
      filesToProcess.foreach(x => {
        val surveyid = x._1
        println("survey id = " + surveyid + " ... file count : " + x._2.length)
        x._2.foreach(surveyfile => {
          val xmlString = hm.readHdfsFile(surveyfile)


          try
          {
              val surveyName = getSurveyDescription(surveyid)

              println("surveyName =" + surveyName)
              if (surveyName.isEmpty) {
                  println("surveyName not found in master file")
                  throw new customDataMapException(s"DataMap Exception -> surveyName not found for surveyid - $surveyid in master file")
              }
              else {
                val parser = new XmlParser(config, xmlString.toString, surveyid, surveyName)
                val contentToSave = parser.parseForDump()
                if (!contentToSave.isEmpty) {
                  println("*********************Content Begins **************************")
                  if (!contentToSave.toString.contains("<Error>FileAlreadyProcessed</Error>")) {
                    //Check if survey id path exists /published/pr/atomicblocks/ab_datamap/{surveyid}
                    val surveyid_published_folder = publishedXMLBaseOutputPathFormat.format(surveyid)
                    if (hm.IsFolderExists(surveyid_published_folder, true)) {

                    }
                    if (parser.surveyVersion == "" || parser.surveyVersion.toLowerCase == "v")
                      parser.surveyVersion = "v1"

                    val newfileName = surveyid_published_folder + "ab_datamap_%s_%s.xml".format(surveyid, parser.surveyVersion)
                    hm.writeHdfsFile(newfileName, contentToSave, true)
                    // Add new entries
                    var newEntry = xmlparse_logInfo("", surveyfile, newfileName, surveyid, parser.surveyVersion)
                    val existingLogEntries = new ArrayBuffer[xmlparse_logInfo]()
                    existingLogEntries.append(newEntry)
                    hm.writeXMLParseLogEntries(stateXMLLogFilePath, existingLogEntries)

                    val processedfilePath = surveys_root_path + processedFilePath.replace("{surveyid}", surveyid) + "p_" + surveyfile.substring(surveyfile.lastIndexOf("/") + 1)
                    hm.moveOriginalXMLFileToProcessed(surveyfile, processedfilePath, true)
                  }
                  else {
                    val processedfilePath = surveys_root_path + processedFilePath.replace("{surveyid}", surveyid) + "up_" + surveyfile.substring(surveyfile.lastIndexOf("/") + 1)
                    hm.moveOriginalXMLFileToProcessed(surveyfile, processedfilePath, true)
                  }
                }
              }
          }
          catch {
            case ex :customDataMapException => {
              println("DataMap Exception while job running - " + ex.getMessage)
            }
            case ex :Exception => {
              println("Exception while job running - " + ex.getMessage)
            }
          }
        })
      })
    }
  }
  catch
    {
      case ex :Exception => {
          printf("Exception while job running - " + ex.getMessage)
      }
    }
    finally {
      hm.fs.close()
    }
  }
}