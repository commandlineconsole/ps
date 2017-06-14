package com.sky.common

import com.sky.conf.ETLConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructField, StructType, _}
import org.apache.spark.sql.{DataFrame, Row}

object DataFrameFunctions {

  import ETLConf._

  def toStringWithNull(str: Any): String = {
    try{str.toString}catch {case _: Throwable => null}
  }

  def melt(df: DataFrame, columns: List[String]): DataFrame ={

    val columnsToKeep = columns.intersect(df.columns)
    val baseSchema = df.schema.fields.filter(sField => columnsToKeep.contains(sField.name))
    val basePositions = columnsToKeep.map(name => df.columns.indexOf(name))

    val schema =
      StructType(baseSchema ++ List(
        StructField("variable", StringType, true),
        StructField("value", StringType, true)))

    val restOfTheColumns =  df.columns.diff(columnsToKeep)
    val dfNames = df.columns

    val rows = df.rdd.flatMap(row => {/** Each wide row becomes several narrow rows */
    val potentialRows = restOfTheColumns.flatMap(variableCol => {

      val colPos = dfNames.indexOf(variableCol)
      val newRow = if (row(colPos) == null) None else Some(Row(basePositions.map(row.get(_)) ++ List(variableCol, row(colPos)) : _*))
      newRow

    })
      if (potentialRows.size == 0) {
        Seq(Row(basePositions.map(row.get(_)) ++ List(null, null): _*))
      }
      else {potentialRows}})

    hiveContext.createDataFrame(rows, schema)

  }

  def createUIDforResponse(df: DataFrame, sid: String) : DataFrame = {
    df.withColumn("createdUid", udf((str: String, surveyid: String)=>

      str + "_" + surveyid).apply(df("uuid"), lit(sid)))
  }

  def responseAutomicTable(df: DataFrame, variableList: List[String], datamap: DataFrame): DataFrame = {
    val variables = df.schema.fieldNames.intersect(variableList).toList
    val newResp = melt(df, variables)

    val toStringUDF  = udf[String, Any]( any => if (any == null) null else toStringWithNull(any))
    val newDatamap =
      datamap.filter(datamap("label").isNotNull && datamap("values_value").isNotNull)
        .withColumn("value_string", toStringUDF(datamap("values_value")))
        .withColumn("dm_version", lit("version"))              // TODO:  new updated column
        .withColumn("dm_remerged", lit("remerged"))           // TODO:  new updated column*/

    import com.sky.common.Dataformat._
    val newResp1 = newResp.
      withColumn("r_version", toStringversion(newResp("markers")))             // TODO:  new updated column
      .withColumn("r_remerged", toStringremerged(newResp("markers")))            // TODO:  new updated column
      .withColumn("rmarkers", regexp_replace(newResp("markers"), "\\,", ","))
      .withColumn("ruserAgent", regexp_replace(newResp("userAgent"), "\\,", ","))
      .withColumn("accnum", lit(null).cast(StringType))

    val joinedResponse = newResp1.
      join(newDatamap,
        newResp1("variable") <=> newDatamap("label") &&   // first join to get the unique records
          newResp1("value") <=> newDatamap("value_string")&& // then join second one.
          newResp1("r_version") <=> newDatamap("version"), "left_outer").distinct()  // left_outer
         .drop("value_string").drop("version")

    val joinedResponsefinal =   joinedResponse.withColumn("Coalesced_Value",
      udf((value: String, title: String) =>
        if (title == null) value else title )
        .apply(
          joinedResponse("value"), joinedResponse("values_title"))).distinct()

    joinedResponsefinal

  }

  def joinTopLevelVariableDescription(df: DataFrame, datamap: DataFrame) ={
    val labels = datamap.select("label").rdd.map(row => row.get(0)).distinct().collect()
    val variables = df.schema.fieldNames.diff(datamap.schema.fieldNames).intersect(labels).diff(Array("date", "ipAddress", "qtime", "record", "session", "start_date","url", "userAgent", "uuid", "markers", "dcua", "list"))

    variables.foldLeft(df)((tempDF: DataFrame, str: String) => {
      val toBeJoined = datamap.filter(datamap("label") === str).select("values_value", "values_title").withColumnRenamed("values_value", "tempForJoin").withColumnRenamed("values_title", str + "_Description").distinct()
      val newDF = tempDF.join(toBeJoined, tempDF(str) === toBeJoined("tempForJoin"),  "left_outer").drop("tempForJoin")
      newDF
    })
  }


  def rename(df: DataFrame, datamap: DataFrame): DataFrame ={
    val colNames = df.schema.fieldNames
    val newColNmaes = colNames.map(name => if (datamap.schema.fieldNames.contains(name)) "datamap_" + name else name)
    val renamedResponseOutput = df.toDF(newColNmaes: _*)
    renamedResponseOutput
  }

  implicit class DataFrameTransform(df: DataFrame){

    def responseAutomicTable(vList: List[String], datamap: DataFrame) = {
      DataFrameFunctions.responseAutomicTable(df, vList, datamap)
    }

    def createUIDforResponse(surveyid: String)={
      DataFrameFunctions.createUIDforResponse(df, surveyid)
    }

    //    def joinTopLevelVariableDescription(datamap: DataFrame)={
    //      DataFrameFunctions.joinTopLevelVariableDescription(df, datamap)
    //    }

    def rename(datamap: DataFrame)={
      DataFrameFunctions.rename(df, datamap)
    }

  }

}
