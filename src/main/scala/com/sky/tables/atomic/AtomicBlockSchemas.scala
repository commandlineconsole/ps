package com.sky.tables.atomic

import org.apache.spark.sql.types._

/**
  * Created by zhenhao.li on 26/11/2016.
  */



/**
  * The idea is that this is the single place to control the atomic block schemas.
  * We use HiveIO to write AVRO files via Hive's AvroSerDe and the Dataframe schemas will be translated to AVRO schemas in a straightforward way.
  * Note that Hive uses bigint instead of long. The translation is in HiveIO.createTable.
  * it is possible to put the schemas in external config files. But it is not safe to do so.
  * Our approach makes the schemas version controlled in the same ETL code base.
  */
//TODO Hive variables are case-insensitive. Rewrite variable names with small letters only.
object AtomicBlockSchemas {

    val datamapSchema = StructType(List(
        StructField("col", StringType, true),
        StructField("colTitle", StringType, true),
        StructField("label", StringType, true),
        StructField("noanswerParent", StringType, true),
        StructField("qlabel", StringType, true),
        StructField("qtitle", StringType, true),
        StructField("row", StringType, true),
        StructField("rowTitle", StringType, true),
        StructField("title", StringType, true),
        StructField("type", StringType, true),
        StructField("vgroup", StringType, true),
        StructField("hasEmptyValues", BooleanType, true),
        StructField("values_title", StringType, true),
        StructField("values_value", LongType, true),
        StructField("source", StringType, true)
    ))


    val responseSchema = StructType(List(
        StructField("date", StringType, true),
        StructField("dcua", StringType, true),
        StructField("hIEversion", StringType, true),
        StructField("ipAddress", StringType, true),
        StructField("list", StringType, true),
        StructField("markers", StringType, true),
        StructField("qtime", StringType, true),
        StructField("record", StringType, true),
        StructField("session", StringType, true),
        StructField("start_date", StringType, true),
        StructField("status", StringType, true),
        StructField("url", StringType, true),
        StructField("userAgent", StringType, true),
        StructField("uuid", StringType, true),
        StructField("vbrowser", StringType, true),
        StructField("vlist", StringType, true),
        StructField("vmobiledevice", StringType, true),
        StructField("vmobileos", StringType, true),
        StructField("vos", StringType, true),
        StructField("createdUid", StringType, true),
        StructField("variable", StringType, true),
        StructField("value", StringType, true),
        StructField("datamap_col", StringType, true),
        StructField("datamap_colTitle", StringType, true),
        StructField("datamap_label", StringType, true),
        StructField("datamap_noanswerParent", StringType, true),
        StructField("datamap_qlabel", StringType, true),
        StructField("datamap_qtitle", StringType, true),
        StructField("datamap_row", StringType, true),
        StructField("datamap_rowTitle", StringType, true),
        StructField("datamap_title", StringType, true),
        StructField("datamap_type", StringType, true),
        StructField("datamap_vgroup", StringType, true),
        StructField("datamap_hasEmptyValues", BooleanType, true),
        StructField("datamap_values_title", StringType, true),
        StructField("datamap_values_value", LongType, true),
        StructField("datamap_source", StringType, true),
        StructField("Coalesced_Value", StringType, true),
        StructField("hIEversion_Description", StringType, true),
        StructField("status_Description", StringType, true),
        StructField("vbrowser_Description", StringType, true),
        StructField("vlist_Description", StringType, true),
        StructField("vmobiledevice_Description", StringType, true),
        StructField("vmobileos_Description", StringType, true),
        StructField("vos_Description", StringType, true)
    ))

}
