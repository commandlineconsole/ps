package com.sky

import com.sky.common.hdfsFileManager
import com.sky.conf.{ETLConf, HDFSConfForFileSystem, JobConfig}
import com.sky.domain._
import com.sky.io.JsonIO
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * Created by zhenhao.li on 04/11/2016.
  */

object AtomicBlockBuilder {
  val jobConfig = new JobConfig("")
  val fsConfig = HDFSConfForFileSystem.getConfForHDFSFileSystem(jobConfig.config)
  val hm = new hdfsFileManager(fsConfig)

  import com.sky.common.DataFrameFunctions._
  /*def getDataMappers(rawDatafilePath:String, numberOfPartitions : Int,xmlDataMapFilePath: String ):  mutable.HashMap[String,mutable.HashMap[String,String]] = {
    var lstMappers = new mutable.HashMap[String,mutable.HashMap[String,String]]()

    var lstfiles = StringBuilder.newBuilder

    try {
      val rawDataResponseDF = JsonIO.getDF(rawDatafilePath, numberOfPartitions).head //.select("markers")
      val marker = rawDataResponseDF.select("markers").distinct()
      val markerFilter = marker.filter(marker("markers").isNotNull).distinct()
      var lstDataMaps = new mutable.HashMap[String, String]()
      markerFilter.collect().foreach( x => {
        val markerString = x.getString(0)
        if(markerString.contains(",")) {
            val marker1 = markerString.split(",")(0)
            if(markerString.contains("_v")) {
              val versionno: String = marker1.substring(marker1.lastIndexOf("_") + 1).replace("v", "")
              val matchingDataMapFilePath: String = hm.getMatchingDataMapFile(xmlDataMapFilePath, versionno.toInt)
              lstDataMaps += versionno -> matchingDataMapFilePath
            }
          }
        }
      )

      lstMappers += rawDatafilePath -> lstDataMaps

      if(lstDataMaps.isEmpty) {
        lstDataMaps.clear()
        val matchingDataMapFilePath: String = hm.getMatchingDataMapFile(xmlDataMapFilePath, 1)
        lstDataMaps += "1" -> matchingDataMapFilePath
        lstMappers += rawDatafilePath -> lstDataMaps
      }

    }
    catch
      {
        case y : Exception => println(s"Error in finding version number in $rawDatafilePath")
      }

    lstMappers
  }*/

  def getJSONResponseDF(datampapath: String,commonVariableResponseTable: List[String], datamap: DataFrame, surveyid : String) : DataFrame = {
    println(s"getJSONResponseDF   $datampapath")
    val responsePriusAutomicWITHuuid = JsonIO.getDFforResponse(datampapath, numberOfPartitions).get.createUIDforResponse(surveyid)
    println(s"responsePriusAutomicWITHuuid ")
    val responsePriusAutomic =   responsePriusAutomicWITHuuid.responseAutomicTable(commonVariableResponseTable, datamap)
    println(s"responsePriusAutomic ")
    responsePriusAutomic
  }


  def getSurveyDescription(surveyid: String, masterFilePath: String) : String = {
    //survey_friendly_name,survey_id
    val surveyNames = hm.readSurveyNamesMaster(masterFilePath)
    val ret = surveyNames.filter(x => x.survey_id.trim.toLowerCase.equals(surveyid.toLowerCase))
    var retValue = ""
  if(!ret.isEmpty)
    retValue = ret.head.survey_friendly_name
  retValue
}


  def getUpdateDataMapDF(datamappath:String) : DataFrame = {
    val datamapPrius_src = ETLConf.hiveContext.read.format("com.databricks.spark.xml").option("rowTag", "record").load(datamappath)
    val datamapPriusSelect = datamapPrius_src.select(
      "col", "colTitle", "colcond", "colwhere", "label", "open", "qalttitle", "qchoicecond", "qcolcond", "qcond", "qgroupby", "qlabel", "qrowcond", "qtitle", "qwhere", "remerged", "row", "rowTitle", "rowcond", "rowwhere", "surveyid", "title", "type", "values_title", "values_value", "version")

    import org.apache.spark.sql.functions._

    val datamapPriusC = datamapPriusSelect
      .withColumn("dmtitle", trim(regexp_replace(regexp_replace(datamapPrius_src("title"), "\\,", ","), " ", " ")))
      .withColumn("Calmonth", lit(null).cast(StringType))
      .withColumn("xWk", lit(null).cast(StringType))
      .withColumn("xWkr1", lit(null).cast(StringType))
      .withColumn("xWkSKY2", lit(null).cast(StringType))
      .withColumn("dmqtitle", trim(regexp_replace(regexp_replace(datamapPrius_src("qtitle"), "\\,", ","), " ", " ")))
     // .withColumn("dm_remerged", lit("remerged"))     // new
     // .withColumn("dm_version", lit("version"))       // new

    datamapPriusC
  }

  def getMaxVersion(filepaths:List[String]) : (Int, String) = {
    val ret = filepaths.map(filepath =>  (filepath.substring(filepath.lastIndexOf("_") +1 , filepath.lastIndexOf(".")).replace("v", "").toInt, filepath)).toList.maxBy(x => x._1)
    ret
  }


  def getLatestDataMapFileDF(sid: String, publishedXMLBaseOutputPath : String) : DataFrame = {
    val filesToProcess = hm.getParsedSurveyXMLFilePathsRecursively(publishedXMLBaseOutputPath, List(sid))
    val latestmaps =  filesToProcess.map( x =>    {
      val latestitem = getMaxVersion(x._2)
      (x._1, latestitem._1, latestitem._2) // surveyid, versionno, file path
    }).toList

    val x = latestmaps.head
    val surveyid = x._1 // bsb
    val latestversionno = "v" + x._2.toString
    val latestdatamapfilepath = x._3

    println("latestdata file path: " + latestdatamapfilepath)
    val finalDataMapDF = getUpdateDataMapDF(latestdatamapfilepath)
    finalDataMapDF.printSchema()
    finalDataMapDF
  }

  var numberOfPartitions : Int = 6
  def main(args: Array[String]): Unit = {
    println("Automic Block Builder starts ")

    numberOfPartitions = args(0).toInt

    import jobConfig._
    /*hdfsSourceDataFileTestPath = "/tmp/dmtest"  // TODO: delete this later
    hdfsSourceProcessedPath = "{surveyid}/data/processed/"  // TODO: delete this later
    hdfsAbResponseFileOutputPath = "/data/published/pr/atomicblocks/ab_response/" // TODO: Check paths

    */

    var lstMappers = new mutable.HashMap[String,List[String]]()
    val stateXMLLogFilePath = jobConfig.config.getString("app.directory.hdfsSurveysXmlparserLogPath")
    val surveys_to_process = jobConfig.config.getStringList("app.surveyidsForXMLParsing.names").toList
    val surveys_root_path: String = jobConfig.config.getString("app.directory.hdfsSourceBasePath")
    val processedFilePath = jobConfig.config.getString("app.directory.hdfsXMLProcessedFilePath")
    val publishedXMLBaseOutputPath = jobConfig.config.getString("app.directory.hdfsPublishedParsedXMLOutputPath")
    val datamapfileformat = jobConfig.config.getString("app.directory.hdfsPublishedParsedXMLOutputPath") + "%s/" // "/data/published/pr/atomicblocks/ab_datamap/"
    val hdfsSourceProcessedPathFormat =  jobConfig.config.getString("app.directory.hdfsSourceProcessedPath") // Moving raw file into processed folder // "{surveyid}/data/processed/"
    val hdfsAbResponseFileOutputPathFormat  =  jobConfig.config.getString("app.directory.hdfsAbResponseFileOutputPath") + "ab_response_%s_%s.txt"
    val hdfsRootDataFilePathFormat = jobConfig.config.getString("app.directory.hdfsRootDataFilePath")  //{surveyid}/data"
    val masterFilePath = config.getString("app.directory.hdfsSurveyNamesMasterFilePath")

    try {

      val dataFilestoProcess = hm.getDataFilePathsRecursively(surveys_root_path, surveys_to_process)

      dataFilestoProcess.foreach(x => {
        val surveyid = x._1

        val surveydescription = getSurveyDescription(surveyid, masterFilePath)

        println("surveyid =" + surveyid)
        x._2.foreach(rawDatafilePath => {
          println("rawDatafilePath =" + rawDatafilePath)
          //Step1 : Get latest datamap for surveyid
          val finalDataMapDF = getLatestDataMapFileDF(surveyid,publishedXMLBaseOutputPath)


          val finalResponseDF = getJSONResponseDF(rawDatafilePath, jobConfig.commonVariableResponseTable, finalDataMapDF, surveyid)
          finalResponseDF.persist()


          val responsePriusAutomicselect = finalResponseDF.select(
            finalResponseDF("xMO"),finalResponseDF("xWk"),finalResponseDF("xWkr1"),finalResponseDF("xWkSKY2"),finalResponseDF("date"),finalResponseDF("dcua"),finalResponseDF("ipAddress"),finalResponseDF("rmarkers"),finalResponseDF("qtime"),finalResponseDF("record"),finalResponseDF("session"),finalResponseDF("start_date"),finalResponseDF("status"),finalResponseDF("url"),finalResponseDF("ruserAgent"),
            finalResponseDF("accnum"),finalResponseDF("uuid"),finalResponseDF("r_remerged"),finalResponseDF("r_version"),finalResponseDF("createdUid"),finalResponseDF("Calmonth"),finalResponseDF("variable"),
            finalResponseDF("value"),finalResponseDF("colTitle"),finalResponseDF("label"),finalResponseDF("qlabel"),finalResponseDF("dmqtitle"),finalResponseDF("qalttitle"),finalResponseDF("rowTitle"),finalResponseDF("dmtitle"),
            finalResponseDF("type"),finalResponseDF("col"),finalResponseDF("row"),finalResponseDF("values_title"),finalResponseDF("values_value"),finalResponseDF("Coalesced_Value"),finalResponseDF("qwhere"),finalResponseDF("rowwhere"),
            finalResponseDF("colwhere"),finalResponseDF("qcond"),finalResponseDF("rowcond"),finalResponseDF("colcond"),finalResponseDF("qrowcond"),finalResponseDF("qcolcond"),finalResponseDF("surveyid")
            ,finalResponseDF("dm_remerged"),finalResponseDF("dm_version")
          )
          // Join 2 select DM -distinct label of map
          val dataMapDF_distinct = finalDataMapDF.select(finalDataMapDF("label"), finalDataMapDF("col"), finalDataMapDF("colTitle"),
            finalDataMapDF("qlabel"), finalDataMapDF("dmqtitle"), finalDataMapDF("qalttitle"),
            finalDataMapDF("row"), finalDataMapDF("rowTitle"), finalDataMapDF("dmtitle"), finalDataMapDF("type"),finalDataMapDF("remerged").as("dm_remerged"),
            finalDataMapDF("surveyid"), finalDataMapDF("version").as("dm_version")).distinct()                                     // TODO : remerged  -- > dm_remerged


          val t1 = dataMapDF_distinct.filter(dataMapDF_distinct("qlabel").equalTo("MOD4Q7"))

          println("dataMapDF_distinct for mod4q7" + t1.count())

          //This set is fine - 1300
          val responsePriusAutomicselectFilterWithNotNullValues = responsePriusAutomicselect.filter(responsePriusAutomicselect("values_value").isNotNull)
            .select(responsePriusAutomicselect("xMO"),
            responsePriusAutomicselect("date"),
            responsePriusAutomicselect("dcua"),
            responsePriusAutomicselect("ipAddress"),
            responsePriusAutomicselect("rmarkers").as("markers"),
            responsePriusAutomicselect("qtime"),
            responsePriusAutomicselect("record"),
            responsePriusAutomicselect("session"),
            responsePriusAutomicselect("start_date"),
            responsePriusAutomicselect("status"),
            responsePriusAutomicselect("url"),
            responsePriusAutomicselect("ruserAgent"),
            responsePriusAutomicselect("uuid"),
            responsePriusAutomicselect("r_remerged"),         // TODO : New col
            responsePriusAutomicselect("r_version"),           // TODO : New col
            responsePriusAutomicselect("createdUid"),
            responsePriusAutomicselect("variable"),
            responsePriusAutomicselect("value"),
            responsePriusAutomicselect("colTitle"),
            responsePriusAutomicselect("label"),
            responsePriusAutomicselect("qlabel"),
            responsePriusAutomicselect("dmqtitle").as("qtitle"),
            responsePriusAutomicselect("qalttitle"),
            responsePriusAutomicselect("rowTitle"),
            responsePriusAutomicselect("dmtitle").as("title"),
            responsePriusAutomicselect("type"),
            responsePriusAutomicselect("col"),
            responsePriusAutomicselect("row"),
            responsePriusAutomicselect("values_title"),
            responsePriusAutomicselect("values_value"),
            responsePriusAutomicselect("Coalesced_Value"),
            responsePriusAutomicselect("qwhere"),
            responsePriusAutomicselect("rowwhere"),
            responsePriusAutomicselect("colwhere"),
            responsePriusAutomicselect("qcond"),
            responsePriusAutomicselect("rowcond"),
            responsePriusAutomicselect("colcond"),
            responsePriusAutomicselect("qrowcond"),
            responsePriusAutomicselect("qcolcond"),
            responsePriusAutomicselect("surveyid"),
            responsePriusAutomicselect("dm_remerged"),  // TODO : New col
            responsePriusAutomicselect("dm_version"),    // TODO : New col
              responsePriusAutomicselect("label").as("reslabel"))

          // this we have to join with distinct datamap - 100
          val responsePriusAutomicselectFilterWithNullValues = responsePriusAutomicselect.filter(responsePriusAutomicselect("values_value").isNull)
          val table1 = responsePriusAutomicselectFilterWithNullValues.registerTempTable("table1")
          val table2 = dataMapDF_distinct.registerTempTable("table2")

          val sqlJoinDF = ETLConf.hiveContext.sql("select table1.xMO,date, table1.dcua,table1.ipAddress,table1.rmarkers as markers,table1.qtime,table1.record,table1.session,table1.start_date,table1.status,table1.url,table1.ruserAgent,table1.uuid," +
            "table1.r_remerged,table1.r_version, table2.dm_remerged,table2.dm_version" +
            ",table1.createdUid,table1.variable,table1.value,table2.colTitle,table2.label,table2.qlabel,table2.dmqtitle qtitle,table2.qalttitle,table2.rowTitle,table2.dmtitle title,table2.type,table2.col," +
            "table2.row,table1.values_title,table1.values_value,table1.Coalesced_Value,table1.qwhere,table1.rowwhere,table1.colwhere,table1.qcond,table1.rowcond," +
            " table1.colcond,table1.qrowcond,table1.qcolcond,table1.surveyid,table1.label reslabel from table1 left join table2 on (lower(trim(table1.variable)) = lower(trim(table2.label)))")
            .toDF("xMO","date","dcua","ipAddress","markers","qtime","record","session","start_date","status","url","userAgent","uuid","r_remerged","r_version","dm_remerged","dm_version","createdUid","variable","value","colTitle","label","qlabel","qtitle","qalttitle","rowTitle","title","type","col","row","values_title","values_value","Coalesced_Value","qwhere","rowwhere","colwhere","qcond","rowcond","colcond","qrowcond","qcolcond","surveyid","reslabel")
          //and lower(trim(table1.rversion)) = lower(trim(table2.dv))
          responsePriusAutomicselectFilterWithNotNullValues.printSchema()
          sqlJoinDF.printSchema()


          val joinedMap = sqlJoinDF.unionAll(responsePriusAutomicselectFilterWithNotNullValues)
          import com.sky.common.Dataformat._


          val joinedMapSID = joinedMap.drop("surveyid")

          val  joinedMapNew = joinedMapSID
            .withColumn("surveyid", toStringUDF(lit(surveyid)))
            .withColumn("newdate", toDateUDF(joinedMapSID("date")))  // .cast("timestamp")
            .withColumn("newstartdate", toDateUDF(joinedMapSID("start_date"))) //.cast("timestamp")
            .withColumn("survey_name", toStringUDF(lit(surveydescription)))
//           . withColumn("dm_version", toStringUDF(lit(surveydescription)))
//           . withColumn("dm_remerged", toStringUDF(lit(surveydescription)))
//          //  .withColumn("newcreateduid", multiplier(surveyid)))


          println("joinedMapNew___________________________________")
          joinedMapNew.printSchema()

          val joinedMapFinal1 = joinedMapNew.drop("date").drop("start_date")

          println("joinedMapFinal1________________________________________")

          joinedMapFinal1.printSchema()

          val joinedMapFinal = joinedMapFinal1
            .withColumnRenamed("newdate", "date")
            .withColumnRenamed("newstartdate", "start_date")
            .withColumnRenamed("col", "collabel")
            .withColumnRenamed("row", "rowlabel")
        //    .withColumnRenamed("row", "rowlabel")
            .drop("reslabel")


          println("joinedMapFinal________________________________________")

          joinedMapFinal.printSchema()
          joinedMapFinal.show(10, truncate=false)

          //joinedMapNew.printSchema()
          //joinedMapNew.show(30)

          println("*************************************************************stop")


          val format = new java.text.SimpleDateFormat("ddMMyyyyHHmmss")
          val datetimeext = format.format(new java.util.Date())
          val atomicPathResponseoutput = hdfsAbResponseFileOutputPathFormat.format(surveyid,datetimeext)
          println("atomicPathResponseoutput =" + atomicPathResponseoutput)
          joinedMapFinal.write.format("com.databricks.spark.avro").mode("overwrite").save(atomicPathResponseoutput)
          val currentRawDataFileFolder = surveys_root_path + hdfsRootDataFilePathFormat.replace("{surveyid}", surveyid)
          val processedFileFolder = surveys_root_path + hdfsSourceProcessedPathFormat.replace("{surveyid}", surveyid)
          val processedfilePath =  processedFileFolder + "p_" + rawDatafilePath.substring(rawDatafilePath.lastIndexOf("/") + 1)
          println("processedfilePath £££££££ == " + processedfilePath)

          //Move files to processed folder
          hm.moveDataFileToProcessed(rawDatafilePath,processedfilePath, true)

          ETLConf.hiveContext.dropTempTable("table2")
          ETLConf.hiveContext.dropTempTable("table1")
        }
        )
      })
    }
    catch {
      case x: customDataMapException => throw new customDataMapException("Matching version not found in DataMap")
      case y : Exception => throw new Exception("Error in finding Matching DataMap file : " + y.getMessage)
    }

    println("Automic Block Builder ends ")
  }
}

