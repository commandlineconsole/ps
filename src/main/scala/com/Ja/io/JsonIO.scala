package com.Ja.io

import org.apache.spark.sql.DataFrame
/**
  * Created by zhenhao.li on 15/11/2016.
  */
object JsonIO {

    import com.Ja.conf.ETLConf._

    def getDF(path: String, numOfPartition: Int): Option[DataFrame] = {
        try {
            val datamapRaw = sc.textFile(path, 8)
            val datamap = hiveContext.read.json(datamapRaw)
            Some(datamap)
            } catch {
                case e: Throwable =>
                    log.error("cannot read in from " + path +" ERROR: " + e.toString)
                    None
        }
    }

    def getDFforResponse(path: String, numOfPartition: Int): Option[DataFrame] = {

        try {
            val datamapRaw = sc.textFile(path, numOfPartition).flatMap {

                case "[" => None
                case "]" => None
                case "\n" => None
                case str => if (str.startsWith(",")) Some(str.drop(1))
                else if (str.endsWith(",")) Some(str.dropRight(1))
                else Some(str)
            }

            val datamap = hiveContext.read.json(datamapRaw)

            Some(datamap)

        } catch {
            case e: Throwable =>
                log.error("cannot read in from " + path +" ERROR: " + e.toString)
                None
        }
    }

}
