package com.Ja.domain


import com.Ja.common.hdfsFileManager
import com.Ja.conf.HDFSConfForFileSystem
import com.typesafe.config.Config

import scala.collection.mutable.ArrayBuffer

/**
  * Created by RCH28 on 18/04/2017.
  */
class xmlParserLog(config: Config) {
    val fsConfig = HDFSConfForFileSystem.getConfForHDFSFileSystem(config)
    val hm = new hdfsFileManager(fsConfig)

    var logEntries:ArrayBuffer[xmlparse_logInfo] = new ArrayBuffer[xmlparse_logInfo]()
    val stateLogPath = config.getString("app.directory.hdfsSurveysXmlparserLogPath")
    logEntries = new ArrayBuffer[xmlparse_logInfo]()
    logEntries = hm.readXMLParseLogEntries(stateLogPath)
    var surveyid:String = ""
    var versionno:String = ""

  def refreshEntries(): Unit =
  {
    logEntries = new ArrayBuffer[xmlparse_logInfo]()
    logEntries = hm.readXMLParseLogEntries(stateLogPath)
  }

  def isFileAlreadyProcessed(surveyid:String,versionno:String): Boolean ={
    this.surveyid = surveyid
    this.versionno = versionno

    var retValue :Boolean = false

    //If file surveyid and version already exists then return true
    if(logEntries.length > 0){
      retValue = logEntries.exists(x => x.version_no == versionno && x.survey_id == surveyid)
    }

    retValue
  }
}
