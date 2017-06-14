package com.sky.common
import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.hadoop.conf.Configuration

import scala.collection.mutable.ListBuffer

//import com.sky.common.hdfsFileManager
import com.sky.domain._
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.collection.mutable.{ArrayBuffer, HashMap}
/**
  * Created by RCH28 on 11/04/2017.
  */

class hdfsFileManager(conf:Configuration) {

  val fs = FileSystem.get(conf);

  // Get the filename out of the file path
    def readAuditLogEntries(filePath: String): ArrayBuffer[audit_logInfo] = {
      import java.io.{BufferedReader, InputStreamReader}
      var logs = new ArrayBuffer[audit_logInfo]()
        val pt = new Path(filePath);
        if (fs.exists(pt)) {
          val br = new BufferedReader(new InputStreamReader(fs.open(pt)));
          var line = ""
          line = br.readLine()
          while (line != null) {
            val item = line.split(",")
            val log = audit_logInfo(item(0), item(1), item(2), item(3), item(4))
            logs += log
            line = br.readLine();
          }
          br.close
        }
      logs
    }


  // Get the filename out of the file path
  def readSurveyNamesMaster(filePath: String): ArrayBuffer[survey_names_master] = {

    import java.io.{BufferedReader, InputStreamReader}

    var survey_names = new ArrayBuffer[survey_names_master]()
    val pt = new Path(filePath);
    if (fs.exists(pt)) {
      val br = new BufferedReader(new InputStreamReader(fs.open(pt)));
      var line = ""
      line = br.readLine()
      while (line != null) {
        val item = line.split(",")
        val survey_name = survey_names_master(item(0), item(1))
        survey_names += survey_name
        line = br.readLine();
      }
      br.close
    }

    survey_names
  }


  def renameAsTempAuditFile(conf: Configuration, filePath: String): ArrayBuffer[audit_logInfo] = {
    var ab =new ArrayBuffer[audit_logInfo]()

    val pt = new Path(filePath)
    //val fs = FileSystem.get(conf)

    if (fs.exists(pt)) {

      ab = readAuditLogEntries(filePath)

      val format = new java.text.SimpleDateFormat("dd_MM_yyyy_HH_mm_ss")
      val datetimeext = format.format(new java.util.Date())
      val retValue = "temp_" + datetimeext + "_" + filePath.substring(filePath.lastIndexOf("/") + 1)

      val newfileName = filePath.substring(0, filePath.lastIndexOf("/")+1) + retValue
      fs.rename(pt, new Path(newfileName))
    }

    ab
  }


    def renameAsTempFile(conf: Configuration, filePath: String): ArrayBuffer[xmlparse_logInfo] = {
      var ab =new ArrayBuffer[xmlparse_logInfo]()

      val pt = new Path(filePath)
      //val fs = FileSystem.get(conf)

      if (fs.exists(pt)) {

        ab = readXMLParseLogEntries(filePath)
        fs.delete(pt, false)
    /*    val format = new java.text.SimpleDateFormat("dd_MM_yyyy_HH_mm_ss")
        val datetimeext = format.format(new java.util.Date())
        val retValue = "temp_" + datetimeext + "_" + filePath.substring(filePath.lastIndexOf("/") + 1)

        val newfileName = filePath.substring(0, filePath.lastIndexOf("/")+1) + retValue
        fs.rename(pt, new Path(newfileName))*/
      }

      ab
    }

    def readXMLParseLogEntries(filePath: String): ArrayBuffer[xmlparse_logInfo] = {
      import java.io.{BufferedReader, InputStreamReader}
      var logs = new ArrayBuffer[xmlparse_logInfo]()
      val pt = new Path(filePath)
      if (fs.exists(pt)) {
        val br = new BufferedReader(new InputStreamReader(fs.open(pt)))
        var line = ""
        line = br.readLine()
        while (line != null) {
          val item = line.split(",")
          val log = xmlparse_logInfo(item(0), item(1), item(2), item(3), item(4))
          logs += log
          line = br.readLine()
        }
        br.close
      }
      logs
    }

  def IsFolderExists(filePath: String, ifNotCreate:Boolean): Boolean = {
    var retValue:Boolean = false
    val pt = new Path(filePath)
    if (!fs.exists(pt)) {
      if(ifNotCreate) {
        fs.mkdirs(pt)
        retValue = true

      }
    }
    else
    {
      retValue = true
    }

    retValue
  }
    def writeAuditLogEntries(fpath:String, newlogs:ArrayBuffer[audit_logInfo]): Boolean = {
      var retValue = false
      try {
        val pt = new Path(fpath)
        var existingEntries = new ArrayBuffer[audit_logInfo]()
        if (fs.exists(pt)) {
          existingEntries = renameAsTempAuditFile(conf, fpath)
          //fs.delete(pt, false)
        }
        val output = fs.create(pt)
        val format = new SimpleDateFormat("yyyy_MM_dd'T'HH:mm:ss")
        val currentDate = format.format(Calendar.getInstance().getTime())

        //val logs = newlogs ++ existingEntries


        existingEntries.foreach(x => {
          output.write((x.lastprocessdate + "," + x.filePath + "," + x.prev_vno + "," + x.new_vno + "," + x.surveryid + "\n").getBytes)
        })

        newlogs.foreach(x => {
          output.write((currentDate + "," + x.filePath + "," + x.prev_vno + "," + x.new_vno + "," + x.surveryid + "\n").getBytes)
        })

        output.hsync()
        retValue = true
      }
      catch
        {
          case exp : Exception => println("Exception in writeAuditLogEntries " + exp.getMessage)
        }
      retValue
    }

    def writeXMLParseLogEntries(fpath:String, newlogs:ArrayBuffer[xmlparse_logInfo]): Boolean ={
      var retValue = false
      try {
        val pt = new Path(fpath)
        var existingEntries = new ArrayBuffer[xmlparse_logInfo]()
        if (fs.exists(pt)) {
          // fs.delete(pt, false)
          existingEntries = renameAsTempFile(conf, fpath)

        }

        val output = fs.create(pt)
        val format = new SimpleDateFormat("yyyy_MM_dd'T'HH:mm:ss")
        val currentDate = format.format(Calendar.getInstance().getTime())

        //val logs = newlogs ++ existingEntries
        existingEntries.foreach(x => {
          output.write((x.log_date + "," + x.raw_filePath + "," + x.parse_xmlfilePath + "," + x.survey_id + "," + x.version_no + "\n").getBytes())
        })

        newlogs.foreach(x => {
          output.write((currentDate + "," + x.raw_filePath + "," + x.parse_xmlfilePath + "," + x.survey_id + "," + x.version_no + "\n").getBytes())
        })
        output.hsync()
        retValue = true
      }
      catch
        {
          case exp : Exception => println("Exception in writeXMLParseLogEntries " + exp.getMessage)
        }

      retValue

    }


  def readHdfsXMLFile( filePath: String): StringBuilder = {
    var sb = StringBuilder.newBuilder
    import java.io.{BufferedReader, InputStreamReader}
    var logs = new ArrayBuffer[audit_logInfo]()
    val pt = new Path(filePath)

    println("readHdfsXMLFile - file  exists1111111111" + filePath)
    if (fs.exists(pt)) {
      println("readHdfsXMLFile - file exists" + filePath)
      val br = new BufferedReader(new InputStreamReader(fs.open(pt)));
      try {
        val str = Stream.continually(br.readLine()).takeWhile(_ != null).mkString
        sb.append(str)
      }
      finally {
          println("closing the stream")
          br.close
      }
    }
    sb
  }


    def readHdfsFile( filePath: String): StringBuilder = {
      var sb = StringBuilder.newBuilder
      import java.io.{BufferedReader, InputStreamReader}
      var logs = new ArrayBuffer[audit_logInfo]()
      val pt = new Path(filePath);
      if (fs.exists(pt)) {
        val br = new BufferedReader(new InputStreamReader(fs.open(pt)));
        try {
          val str = Stream.continually(br.readLine()).takeWhile(_ != null).mkString("\n")
          sb.append(str)
        }
        finally {
        //  br.close
        }
      }
      sb
    }

    def moveOriginalXMLFileToProcessed(source: String, destination: String, createIfNotExists : Boolean): Unit ={
      val sp = new Path(source)
      val dp = new Path(destination)

      val destfolder = new Path(destination.substring(0, destination.lastIndexOf("/")))
      if(!fs.exists(destfolder) && createIfNotExists)
        {
          fs.mkdirs(destfolder)
        }

      fs.rename(sp, dp)
    }

    def writeHdfsFile(filePath:String, sb: StringBuilder, overwrite: Boolean) = {
      val pt = new Path(filePath)
      if(fs.exists(pt)) {
        if(overwrite) {
          //renameAsTempFile(conf, filePath)
          fs.delete(pt, false)
        }
        else {
          throw new Exception("File already exists..Cannot overwrite")
        }
      }
      val output = fs.create(pt)
      output.write(sb.toString().getBytes)
      output.hsync()
      output.close()

    }

    def getSurveyXMLFilePathsRecursively(rootPath:String, surveys_to_process:List[String]): HashMap[String, List[String]] ={
      var fielstobeProcessed = new HashMap[String, List[String]]()
      println("getSurveyXMLFilePathsRecursively = "  + rootPath)
      val status = fs.listStatus(new Path(rootPath))

      status.foreach(x=> {

        val localpath = x.getPath.toString
        var foldername = localpath.substring(localpath.lastIndexOf("/") + 1)
        if(surveys_to_process.length > 0 && surveys_to_process.contains(foldername)) {
          val lstfiles = getSurveyXMLPaths(localpath , "/surveyxml")
          fielstobeProcessed += (foldername -> lstfiles)
        }
        else if(surveys_to_process.length <= 0)
          {
            val lstfiles = getSurveyXMLPaths(localpath,  "/surveyxml")
            fielstobeProcessed += (foldername -> lstfiles)
          }
      })

      fielstobeProcessed
    }


  def getParsedSurveyXMLFilePathsRecursively(rootPath:String, surveys_to_process:List[String]): HashMap[String, List[String]] ={
    var fielstobeProcessed = new HashMap[String, List[String]]()
    val status = fs.listStatus(new Path(rootPath))
    status.foreach(x=> {
      val localpath = x.getPath.toString
      var foldername = localpath.substring(localpath.lastIndexOf("/") + 1)
      if(surveys_to_process.length > 0 && surveys_to_process.contains(foldername)) {
        val lstfiles = getSurveyXMLPaths(localpath, "")
        fielstobeProcessed += (foldername -> lstfiles)
      }
      else if(surveys_to_process.length <= 0)
      {
        val lstfiles = getSurveyXMLPaths(localpath, "")
        fielstobeProcessed += (foldername -> lstfiles)
      }
    })

    fielstobeProcessed
  }



    def getSurveyXMLPaths(surveyPath:String, subFolder: String) : List[String] = {
      var lstfiles = ListBuffer[String]()
      //if(!surveyPath.toLowerCase.contains(".txt")) {
        val newPath = surveyPath + subFolder
        //println("getSurveyXMLPaths  =  newPath: "  + newPath)
       // try {
          if (fs.exists(new Path(newPath))) {

            val status = fs.listStatus(new Path(newPath))
            status.foreach(x => {
              if(!fs.isDirectory(x.getPath)) {
                //get path string and get the final foldername
                lstfiles += x.getPath.toString
               // println("getSurveyXMLPaths  =  x.getPath.toString: "  + x.getPath.toString)
              }
            })
          }
      /*  }
        catch
          {

            case e: Exception => println("Exception while accessing the path : " + newPath + " \r\n" + e.getMessage)
          }*/
      //}

      lstfiles.toList
    }

  def moveDataFileToProcessed(source: String, destination: String, createIfNotExists: Boolean): Unit = {

    val sp = new Path(source)
    val dp = new Path(destination)

    val processedfolder = destination.substring(0,destination.lastIndexOf("/"))
    println("processedfolder ===== " + processedfolder)
    if (!fs.exists(new Path(processedfolder)) && createIfNotExists) {
      fs.mkdirs(new Path(processedfolder))
    }
    fs.rename(sp, dp)
  }

  def getDataFilePathsRecursively(rootPath: String, surveys_to_process: List[String]):  HashMap[String, List[String]] = {

    var fielstobeProcessed = new HashMap[String, List[String]]()
    val status = fs.listStatus(new Path(rootPath))

    status.foreach(x => {
      val localpath = x.getPath.toString
      var foldername = localpath.substring(localpath.lastIndexOf("/") + 1)
      if (surveys_to_process.length > 0 && surveys_to_process.contains(foldername)) {
        val lstfiles = getSurveyXMLPaths(localpath, "/data")
        fielstobeProcessed += (foldername -> lstfiles)
      }
      else if (surveys_to_process.length <= 0) {
        val lstfiles = getSurveyXMLPaths(localpath, "/data")
        fielstobeProcessed += (foldername -> lstfiles)
      }
    })

    fielstobeProcessed

  }

  def getMatchingDataMapFile(surveyidpath: String, versionNo: Int) : String = {
    var ret = ""
    try {
        var availableVersions = new HashMap[Int, String]()
        val filesToProcess = getSurveyXMLPaths(surveyidpath, "")
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


        //availableVersions.foreach(x => println("striped out version: " + x))

    }
    catch {
      case x: customDataMapException => throw new customDataMapException("Matching version not found in DataMap")
      case y : Exception => throw new Exception("Error in finding Matching DataMap file : " + y.getMessage)
    }

    ret
  }

}
