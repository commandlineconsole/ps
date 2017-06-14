package com.Ja.conf

import java.io.File

import com.Ja.conf.ETLConf.log
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.UserGroupInformation
import org.slf4j.LoggerFactory
import scala.collection.JavaConversions._
/**
  * Created by zhenhao.li on 02/11/2016.
  */
class JobConfigBase(configFilePath: String) {

    //val log = LoggerFactory.getLogger(this.getClass.getName)
    val config: Config = if(configFilePath.isEmpty) ConfigFactory.load("application") else ConfigFactory.parseFile(new File(configFilePath))
}



class JobConfig(configFilePath: String) extends JobConfigBase(configFilePath){

    /* input path for survey folders */
    val surveysPath = config.getString("hdfs.surveys.path")
    /* input path for survey folders */


    //val surveysPublishPath = config.getString("hdfs.surveys.publish.path")
    /* input path for audit output */
    //val auditsOutputPath = config.getString("hdfs.surveys.audit_log.path")
    /* input path for audit output */
    val auditsLogFilePath = config.getString("hdfs.surveys.audit_log.logfile_path")
    /* XML Parser log file */
    //val xmlparser_log = config.getString("hdfs.surveys.xmlparser_log.path")
    /** input path of raw datamap input for prius */
    //val rawPathPriusDatamap = config.getString("input.raw.prius.datamap.path")
   /* /** parallelism of reading the files */
    val pNumPriusDatamap = config.getInt("input.raw.prius.datamap.p_num")

    /** input path of raw response input for prius */
    val rawPathPriusResponse = config.getString("input.raw.prius.response.path")
    /** parallelism of reading the files */
    val pNumPriusResponse = config.getInt("input.raw.prius.response.p_num")

    import collection.JavaConversions._
    /** input path of raw datamap input for toyota */
    val rawPathToyotaDatamap = config.getString("input.raw.toyota.datamap.path")
    /** parallelism of reading the files */
    val pNumToyotaDatamap = config.getInt("input.raw.toyota.datamap.p_num")

    /** input path of raw response input for toyota */
    val rawPathToyotaResponse = config.getString("input.raw.toyota.response.path")
    /** parallelism of reading the files */
    val pNumToyotaResponse = config.getInt("input.raw.toyota.response.p_num")
*/
    /** top level variables in the response atomic block */
    val commonVariableResponseTable = config.getStringList("atomic.response.base_variables").toList
/*
    /** input path of the mapping/change-log */
    val mappingTSVpath = config.getString("input.raw.mapping.path")

    /** Output paths of atomic blocks*/
    val atomicPathDatamap = config.getString("atomic.datamap.path")
    val atomicPathResponse = config.getString("atomic.response.path")*/

    def getConfigValue(key:String) : String = {
        var configValue = ""

        try {
            configValue = config.getString(key)
        }
        catch

            {case e: Exception =>
                println(s"Exception in getting config Value $key")
            }

        configValue
    }

}
