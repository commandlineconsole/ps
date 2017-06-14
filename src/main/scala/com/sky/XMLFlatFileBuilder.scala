package com.sky

import java.io.{BufferedWriter, File, FileWriter}

import com.sky.conf.JobConfig
import com.sky.io.HiveIO
import com.sky.io.JsonIO
import com.sky.tables.atomic.AtomicBlockDatamap
import com.sky.tables.atomic.AtomicBlockResponse
import org.apache.spark
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SaveMode}
import com.sky.conf.ETLConf
import com.sky.conf.ETLConf.hiveContext
import com.sky.domain._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext

import scala.collection.mutable.ArrayBuffer
import scala.xml.NodeSeq
/**
  * Created by RCH28 on 06/04/2017.
  * com.sky.XMLFlatFileBuilder
  */
object XMLFlatFileBuilder {

  def caseClasstoXML(a: XMLParserOutput): NodeSeq = {
    val x = <record>
              <version>{a.version}</version>
              <remerged>{a.remerged}</remerged>
              <surveyid>{a.surveyid}</surveyid>
              <type>{a.fieldtype}</type>
              <qlabel>{a.qlabel}</qlabel>
              <qtitle>{a.qtitle}</qtitle>
              <qalttitle>{a.qalttitle}</qalttitle>
              <qcond>{a.qcond}</qcond>
              <qrowcond>{a.qrowcond}</qrowcond>
              <qcolcond>{a.qcolcond}</qcolcond>
              <qchoicecond>{a.qchoicecond}</qchoicecond>
              <qwhere>{a.qwhere}</qwhere>
              <row>{a.row}</row>
              <col>{a.col}</col>
              <label>{a.label}</label>
              <rowTitle>{a.rowTitle}</rowTitle>
              <colTitle>{a.colTitle}</colTitle>
              <title>{a.title}</title>
              <values_title>{a.values_title}</values_title>
              <values_value>{a.values_value}</values_value>
              <altRowTitle>{a.altRowTitle}</altRowTitle>
              <altColTitle>{a.altColTitle}</altColTitle>
              <rowcond>{a.rowcond}</rowcond>
              <colcond>{a.colcond}</colcond>
              <rowwhere>{a.rowwhere}</rowwhere>
              <colwhere>{a.colwhere}</colwhere>
              <qgroupby>{a.qgroupby}</qgroupby>
              <open>{a.open}</open>
              <value>{a.value}</value>
    </record>


    x
  }

  def main(args: Array[String]): Unit = {

   /*
    working code for local files

    import ETLConf.hiveContext.implicits._
    val par = new XmlParser("","bsb14022")
    val output: ArrayBuffer[XMLParserOutput] = par.parseForDump()

    val t = <records>
              {output.map( x => caseClasstoXML(x))}
            </records>

    val bw = new BufferedWriter(new FileWriter(new File("xml_response_27.txt")))
    bw.write(t.mkString)
    bw.close()*/

    //var rdd = ETLConf.sc.parallelize(output, 1) //.take(20)//.map ( p => RDDXMLOutput(p.version,p.remerged,p. fieldtype,p.qlabel,p.qtitle,p.qalttitle,p.qcond,p.qrowcond,p.qcolcond,p.qchoicecond,p.qwhere,p.row,p.col,p.label,p.rowTitle,p.colTitle,p.title,p.values_title,p.values_value,p.altRowTitle,p.altColTitle,p.rowcond,p.colcond ,p.rowwhere,p.colwhere,p.qgroupby,p.open,p.value))

    ///val df = rdd.map( p => (RDDXMLOutput1(p.version,p.remerged,p.surveyid, p. fieldtype,p.qlabel,p.qtitle,p.qalttitle,p.qcond,p.qrowcond,p.qcolcond,p.qchoicecond,p.qwhere),  RDDXMLOutput2(p.row,p.col,p.label,p.rowTitle,p.colTitle,p.title,p.values_title,p.values_value,p.altRowTitle,p.altColTitle,p.rowcond,p.colcond ,p.rowwhere,p.colwhere,p.qgroupby,p.open,p.value))).toDF()
    //val df = rdd.map( p => p.rootnode + "|" + p.version + "| " + p.remerged  + "| " + p.surveyid  + "|" + p. fieldtype + "|" + p.qlabel + "|" + p.qtitle + "|" + p.qcond + "|" + p.qrowcond + "|" + p.qcolcond + "|" + p.qchoicecond + "|" + p.qwhere + "|" + p.row + "|" + p.rowTitle + "|" + p.col + "|" + p.label + "|" + p.colTitle + "|" + p.values_title + "|" + p.values_value + "|" + p.altRowTitle + "|" + p.altColTitle + "|" + p.rowcond + "|" + p.colcond  + "|" + p.rowwhere + "|" + p.colwhere + "|" + p.qgroupby + "|" + p.open + "|" + p.value).saveAsTextFile("c:\\ravi\\data\\results12.txt")

    ///df.write.format("json").save("c:\\ravi\\data\\results40.txt")
    //df.write.mode(SaveMode.Append).format("json").save("c:\\ravi\\results6.txt")


/********

    val appConfigPath: String = args(0)
    val jobConfig = new JobConfig(appConfigPath)
    import jobConfig._
    val fs = FileSystem.get(ETLConf.sc.hadoopConfiguration)
    val status = fs.listStatus(new Path(jobConfig.surveysPath))
    status.foreach(x=> println(x.getPath))
  ***/
/******************************
    val rdd = ETLConf.sc.textFile("C:\\Ravi\\Source\\Primary_Research_Strategic_Data\\primary_research_karthik\\data\\surveyxml\\decrypt_decipherapi_surveyxml_160823_20170119161400.xml")
    val srdd = rdd.map( x => x)
    val builder = new StringBuilder()
    srdd.collect().foreach(
        x => {
          builder.append(x)
        }
    )

    println(builder.toString())
************************************/

   // status[0].
    /*



    javaRDD.foreachPartition(partition -> {
  StringBuilder builder = new StringBuilder();
  while (partition.hasNext()) {
    builder.append(partition.next());
  }
  System.out.println(builder.toString() + " ****");
});
      //  ETLConf.hiveContext.clearCache()
        //val hc = new HiveContext(ETLConf.sc)
        val df = ETLConf.hiveContext.read.format("com.databricks.spark.xml")
          .option("rowTag", "style")
          //.option("rowTag", "radio")
          .option("nullValue", "null")
          .option("treatEmptyValuesAsNulls", "true")
          .load("C:\\Ravi\\Source\\Primary_Research_Strategic_Data\\primary_research_karthik\\data\\surveyxml\\decrypt_decipherapi_surveyxml_160823_20170119161400.xml")


        InputStream is = fs.open(inputPath);
        OutputStream os = new BufferedOutputStream(new FileOutputStream(localOutputPath));
        IOUtils.copyBytes(is, os, conf);

        df.printSchema()
        df.registerTempTable("datatable")

        ETLConf.hiveContext.sql("""select * from datatable""").show(5)
    */



  }
}