package com.Ja.io

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}

/**
  * Created by zhenhao.li on 24/11/2016.
  */
object IOUtil {

    import com.Ja.conf.ETLConf._

    def readMappingData(path: String): Option[DataFrame]={
        try {
            val mappingRaw = sc.textFile(path)
            val schema = StructType(List(StructField("Survey_Name", StringType, true), StructField("Question_Label", StringType, true), StructField("Meta_Label", StringType, true),StructField("Date", StringType, true), StructField("Source", StringType, true)))

            val mappingdata = mappingRaw.map(line =>  Row.fromSeq(line.split("\t", -1)))
            //mappingdata.foreach(r => println(r.toString))
            val df = hiveContext.createDataFrame(mappingdata, schema)
            //df.show()
            Some(df)

        } catch {
            case e: Throwable =>
                log.error("cannot read in from " + path +" ERROR: " + e.toString)
                None
        }
    }

}
