package com.sky.conf

/**
  * Created by zhenhao.li on 01/11/2016.
  */

import com.typesafe.config.Config
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.UserGroupInformation
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import org.slf4j.LoggerFactory
import org.apache.spark.sql.SQLContext


/**
  * To configure the Spark job
  *
  */

trait Conf {
    val conf: SparkConf
    val hiveContext: SQLContext
}


/**
  * allowMultipleContexts is set on true. But ideally every processing that needs a Spark context should use the same ETLConf object.
  * to use it, put
  * import com.sky.conf.ETLConf._
  * wherever needs a Spark context
  */
object ETLConf extends Conf{
    val conf = new SparkConf()
      .setAppName("Spark ETL Job").setMaster("local[1]")
      /*.set("spark.sql.sources.default", "json")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.sql.tungsten.enabled", "true")
      .set("spark.driver.allowMultipleContexts", "true")*/

    val sc = new SparkContext(conf)
    val hiveContext = new SQLContext(sc)

    import hiveContext.implicits._
        // Logging
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    // Enable snappy compression for Avro
 //   hiveContext.setConf("spark.sql.avro.compression.codec", "snappy")

    val log = LoggerFactory.getLogger(this.getClass.getName)
}

object HDFSConfForFileSystem
{
    val log = LoggerFactory.getLogger(this.getClass.getName)
    def getConfForHDFSFileSystem(config: Config): Configuration = {
        System.setProperty("HADOOP_USER_NAME", config.getString("app.directory.hdfsUser"))
        System.setProperty("spark.ui.showConsoleProgress", "false")
        Logger.getLogger("org").setLevel(Level.WARN)
        Logger.getLogger("akka").setLevel(Level.WARN)
        val conf = new Configuration()
        conf.set("fs.defaultFS", config.getString("app.directory.hdfsDefaultFs"))
        conf.set("dfs.datanode.kerberos.principal", config.getString("app.directory.hdfsDataNodeKerberosPrincipal"))
        conf.set("dfs.namenode.kerberos.principal", config.getString("app.directory.hdfsNameNodeKerberosPrincipal"))
        conf.set("hadoop.security.authentication", "Kerberos")
        UserGroupInformation.setConfiguration(conf)
        UserGroupInformation.loginUserFromKeytab(config.getString("app.directory.hdfsUser") + "@" + config.getString("app.directory.hdfsUserDomain"),
            config.getString("app.directory.hdfsUserKeyTabFile"))

        conf
    }

}
