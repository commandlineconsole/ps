package com.sky.ioXMLParser
import com.sky.conf.ETLConf
/**
  * Created by RCH28 on 28/03/2017.
  */
object bdXMLParser {

  def main(args: Array[String]): Unit = {

    val df = ETLConf.hiveContext.read
      .format("com.databricks.spark.xml")
      .option("rowTag", "radio")
      .load("C:\\Ravi\\Source\\Primary_Research_Strategic_Data\\primary_research_karthik\\pirus.xml")
     df.printSchema()


  }


}
