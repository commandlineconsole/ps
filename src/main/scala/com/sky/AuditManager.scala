package com.sky
import com.sky.common.hdfsFileManager
import com.sky.conf._
import com.sky.domain._
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.collection.JavaConversions._
import scala.collection.mutable.{ArrayBuffer, _}

/**
  * Created by RCH28 on 12/04/2017.
  */

case class questionAttrs(oversion:String,nversion:String,oqlabel:String, oqtitle:String,
                         nqtitle:String, oqalttitle:String ,
                         nqalttitle:String,oqwhere:String,nqwhere:String,
                         oqcond:String,nqcond:String,oqtype:String, nqtype:String)
case class auditoutput (changedate: String, NameofSurvey:String)

case class audit_changetypes()
{
  val Question_added = "Question added"
  val Question_deleted = "Question deleted"
  val Question_type_changed = "Question type changed"
  val Question_text_changed :String = "Question text changed"
  val Question_hidden = "Question hidden"
  val Question_reinstated = "Question reinstated"
  val Row_added = "Row added"
  val Row_deleted = "Row deleted"
  val Row_text_changed = "Row text changed"
  val Row_hidden = "Row hidden"
  val Row_reinstated = "Row reinstated"
  val Column_added = "Column added"
  val Column_deleted= "Column deleted"
  val Column_text_changed = "Column text changed"
  val Column_hidden = "Column hidden"
  val Column_reinstated = "Column reinstated"
  val Response_option_added = "Response option added"
  val Response_deleted = "Response deleted"
  val Response_text_changed = "Response text changed"
  val Response_hidden = "Response hidden"
  val Response_reinstated = "Response reinstated"
}


object AuditManager {

    def comparetexts(v1: String, v2: String, fieldName:String) : String = {
        var retValue = ""

      val val1 : String = if(v1 == null) "" else v1
      val val2 : String= if(v2 == null) "" else v2

        if(!val1.equals(val2)) {
          retValue = s"$fieldName - from <$val1> to <$val2>"
        }

        retValue
    }

    def getLatestAuditEntries(config: Config, filePath : String) : ArrayBuffer[audit_logInfo] = {

        val fsConfig = HDFSConfForFileSystem.getConfForHDFSFileSystem(config)
        val hm = new hdfsFileManager(fsConfig)

        val ret = new ArrayBuffer[audit_logInfo]()
        val entries = hm.readAuditLogEntries(filePath)

        if(entries.length > 0) {

            val output = entries.map( x => (x.surveryid, x.new_vno.toInt)).groupBy( x => x._1).map( x => (x._1, x._2.toList.max)).map(x => (x._1,x._2._2))

            entries.foreach( x =>
               {
                   if(output.filter( y => (y._1.equals(x.surveryid) && (y._2.equals(x.new_vno.toInt)))).nonEmpty) {
                      ret += x
                   }
               }
            )
        }
      else
        {
          println("no entries in audit log")
        }

        ret
    }

    def getInnerJoinerQuestions(dfOld: DataFrame, dfNew: DataFrame): DataFrame ={
      val innerJoiner = dfOld.as("o").join(dfNew.as("n"),dfNew("qlabel") === dfOld("qlabel"), "inner" ).select(dfOld("version").as("oversion"), dfNew("version").as("nversion"), dfOld("qlabel").as("oqlabel"), dfOld("qtitle").as("oqtitle"),dfNew("qtitle").as("nqtitle"), dfOld("qalttitle").as("oqalttitle"),
        dfNew("qalttitle").as("nqalttitle"), dfOld("qwhere").as("oqwhere"),dfNew("qwhere").as("nqwhere"),dfOld("qcond").as("oqcond"), dfNew("qcond").as("nqcond"),dfOld("type").as("oqtype"), dfNew("type").as("nqtype")).distinct().toDF()

      innerJoiner
    }


    def getLeftJoinerQuestions(dfOld: DataFrame, dfNew: DataFrame): DataFrame ={
      val oldLeftJoinNew = dfOld.as("o").join(dfNew.as("n"),dfNew("qlabel") === dfOld("qlabel"), "left_outer" ).select(dfOld("version").as("oversion"), dfNew("version").as("nversion"), dfOld("qlabel").as("oqlabel"), dfOld("qtitle").as("oqtitle"),dfNew("qtitle").as("nqtitle"), dfOld("qalttitle").as("oqalttitle"),
        dfNew("qalttitle").as("nqalttitle"), dfOld("qwhere").as("oqwhere"),dfNew("qwhere").as("nqwhere"),dfOld("qcond").as("oqcond"), dfNew("qcond").as("nqcond"),dfOld("type").as("oqtype"), dfNew("type").as("nqtype")).distinct().toDF()
      oldLeftJoinNew
    }

  def getRightJoinerQuestions(dfOld: DataFrame, dfNew: DataFrame): DataFrame ={
    val oldRightJoinNew = dfOld.as("o").join(dfNew.as("n"),dfNew("qlabel") === dfOld("qlabel"), "right_outer" ).select(dfOld("version").as("oversion"), dfNew("version").as("nversion"), dfOld("qlabel").as("oqlabel"), dfOld("qtitle").as("oqtitle"),dfNew("qtitle").as("nqtitle"), dfOld("qalttitle").as("oqalttitle"),
      dfNew("qalttitle").as("nqalttitle"), dfOld("qwhere").as("oqwhere"),dfNew("qwhere").as("nqwhere"),dfOld("qcond").as("oqcond"), dfNew("qcond").as("nqcond"),dfNew("qlabel").as("nqlabel"),dfOld("type").as("oqtype"), dfNew("type").as("nqtype"))
      .distinct().toDF()
    oldRightJoinNew
  }


  def getHiddenQuestions(dfOld: DataFrame, dfNew: DataFrame): DataFrame ={

    val df_oldDFH = dfOld.filter(dfOld("qwhere").contains("execute") || dfOld("qwhere").contains("report")
      || dfOld("qwhere").contains("none") || dfOld("qwhere").contains("data") || dfOld("qwhere").contains("summary")
      || dfOld("qcond").contains("0") || dfOld("qcond").contains(" 0")|| dfOld("qcond").contains("0\"")
    )

    val df_newDFH = dfNew.filter(dfNew("qwhere").contains("execute") || dfNew("qwhere").contains("report")
      || dfNew("qwhere").contains("none") || dfNew("qwhere").contains("data") || dfNew("qwhere").contains("summary")
      || dfNew("qcond").contains("0") || dfNew("qcond").contains(" 0")|| dfNew("qcond").contains("0\"")
    )

    val rightFilteredDFRaw = df_oldDFH.as("o").join(df_newDFH.as("n"),df_newDFH("qlabel") === df_oldDFH("qlabel"), "right_outer" ).select(df_oldDFH("version").as("oversion"), df_newDFH("version").as("nversion"), df_oldDFH("qlabel").as("oqlabel"), df_oldDFH("qtitle").as("oqtitle"),df_newDFH("qtitle").as("nqtitle"), df_oldDFH("qalttitle").as("oqalttitle"),
      df_newDFH("qalttitle").as("nqalttitle"), df_oldDFH("qwhere").as("oqwhere"),df_newDFH("qwhere").as("nqwhere"),df_oldDFH("qcond").as("oqcond"), df_newDFH("qcond").as("nqcond"),df_newDFH("qlabel").as("nqlabel"),df_oldDFH("type").as("oqtype"), df_newDFH("type").as("nqtype"))
      .distinct().toDF()

    val rightFilteredDF = rightFilteredDFRaw.filter(rightFilteredDFRaw("oversion").isNull)
    val rdf_with_type_1 = rightFilteredDF.withColumn("hidtype",lit(audit_changetypes().Question_hidden).cast(StringType))


    val leftFilteredDFRaw = df_oldDFH.as("o").join(df_newDFH.as("n"),df_newDFH("qlabel") === df_oldDFH("qlabel"), "left_outer" ).select(df_oldDFH("version").as("oversion"), df_newDFH("version").as("nversion"), df_oldDFH("qlabel").as("oqlabel"), df_oldDFH("qtitle").as("oqtitle"),df_newDFH("qtitle").as("nqtitle"), df_oldDFH("qalttitle").as("oqalttitle"),
      df_newDFH("qalttitle").as("nqalttitle"), df_oldDFH("qwhere").as("oqwhere"),df_newDFH("qwhere").as("nqwhere"),df_oldDFH("qcond").as("oqcond"), df_newDFH("qcond").as("nqcond"),df_newDFH("qlabel").as("nqlabel"),df_oldDFH("type").as("oqtype"), df_newDFH("type").as("nqtype"))
      .distinct().toDF()

    val leftFilteredDF = leftFilteredDFRaw.filter(leftFilteredDFRaw("nversion").isNull)

    val rdf_with_type_2 = leftFilteredDF.withColumn("hidtype",lit(audit_changetypes().Question_reinstated).cast(StringType))

    val rdf_with_type = rdf_with_type_1.unionAll(rdf_with_type_2)

    rdf_with_type

  }

  def getColChanges(dfO: DataFrame, dfN: DataFrame): DataFrame = {
    //get row records
    val dfOld = dfO.filter(dfO("col").isNotNull)
    val dfNew = dfN.filter(dfN("col").isNotNull)

    val oldRightJoinNew = dfOld.as("o").join(dfNew.as("n"),dfNew("qlabel") === dfOld("qlabel") and dfNew("col") === dfOld("col") , "right_outer" ).select(dfNew("qlabel").as("qlabel"),
      dfOld("version").as("oversion"), dfNew("version").as("nversion"), dfNew("qtitle").as("qtitle"),dfNew("colTitle").as("colTitle")).distinct().toDF()
    val oldRightJoinNewFiltered = oldRightJoinNew.filter(oldRightJoinNew("oversion").isNull )
    val newCols = oldRightJoinNewFiltered.withColumn("changetype",lit(audit_changetypes().Column_added).cast(StringType))

    //get row deleted
    val oldLeftJoinNew = dfOld.as("o").join(dfNew.as("n"),dfNew("qlabel") === dfOld("qlabel") and dfNew("col") === dfOld("col"), "left_outer" ).select(dfOld("qlabel").as("qlabel"),
      dfOld("version").as("oversion"), dfNew("version").as("nversion"), dfOld("qtitle").as("qtitle"),dfOld("colTitle").as("colTitle")).distinct().toDF()



    val oldLeftJoinNewFiltered = oldLeftJoinNew.filter(oldLeftJoinNew("nversion").isNull ).distinct()


    val deletedCols = oldLeftJoinNewFiltered.withColumn("changetype",lit(audit_changetypes().Column_deleted).cast(StringType))

    deletedCols.filter(oldLeftJoinNew("qlabel").contains("S13")) .take(1000).foreach(println)
    println("deletedCols =" + deletedCols.count())

    //Hidden questions
    /***********************************************************************/
    deletedCols.take(1000).foreach(println)

    val df_oldDFH = dfOld.filter(dfOld("colwhere").contains("execute") || dfOld("colwhere").contains("report")
      || dfOld("colwhere").contains("none") || dfOld("colwhere").contains("data") || dfOld("colwhere").contains("summary")
      || dfOld("colwhere").contains("0") || dfOld("colwhere").contains(" 0")|| dfOld("colwhere").contains("0\"")
    )

    val df_newDFH = dfNew.filter(dfNew("colwhere").contains("execute") || dfNew("colwhere").contains("report")
      || dfNew("colwhere").contains("none") || dfNew("colwhere").contains("data") || dfNew("colwhere").contains("summary")
      || dfNew("colwhere").contains("0") || dfNew("colwhere").contains(" 0")|| dfNew("colwhere").contains("0\"")
    )

    //New row Hidden but not present in old data
    val rightFilteredDFRaw = df_oldDFH.as("o").join(df_newDFH.as("n"),df_newDFH("qlabel") === df_oldDFH("qlabel") and df_newDFH("col") === df_oldDFH("col"), "right_outer" ).
      select(dfNew("qlabel").as("qlabel"),dfOld("version").as("oversion"), dfNew("version").as("nversion"), dfNew("qtitle").as("qtitle"),dfNew("colTitle").as("colTitle")).distinct().toDF()

    val rightFilteredDF = rightFilteredDFRaw.filter(rightFilteredDFRaw("oversion").isNull)
    val rdf_with_type_1 = rightFilteredDF.withColumn("changetype",lit(audit_changetypes().Column_hidden).cast(StringType))


    val leftFilteredDFRaw = df_oldDFH.as("o").join(df_newDFH.as("n"),df_newDFH("qlabel") === df_oldDFH("qlabel")  and df_newDFH("col") === df_oldDFH("col"), "left_outer" ).
        select(dfOld("qlabel").as("qlabel"),dfOld("version").as("oversion"), dfNew("version").as("nversion"), dfOld("qtitle").as("qtitle"),dfOld("colTitle").as("colTitle")).distinct().toDF()
    val leftFilteredDF = leftFilteredDFRaw.filter(leftFilteredDFRaw("nversion").isNull)
    val rdf_with_type_2 = leftFilteredDF.withColumn("changetype",lit(audit_changetypes().Column_reinstated).cast(StringType))


    /***************************************  end ***************************************************/


    //reinstated questions



    val finalds = newCols.unionAll(deletedCols).unionAll(rdf_with_type_1).unionAll(rdf_with_type_2)

    finalds

  }


  def getvalues_valueChanges(dfOld: DataFrame, dfNew: DataFrame): DataFrame = {
    //TODO: Add hidden logic
    //get new response records

    val oldRightJoinNew = dfOld.as("o").join(dfNew.as("n"),dfNew("qlabel") === dfOld("qlabel") and dfNew("values_label") === dfOld("values_label"), "right_outer" ).select(dfNew("qlabel").as("qlabel"),
      dfOld("version").as("oversion"), dfNew("version").as("nversion"), dfNew("qtitle").as("qtitle"),dfOld("values_value").cast(StringType).as("ovalues_value"),dfNew("values_value").cast(StringType).as("nvalues_value")).distinct().toDF()
    val oldRightJoinNewFiltered1 = oldRightJoinNew.filter(oldRightJoinNew("oversion").isNull)

    println("oldRightJoinNewFiltered1 count  = " + oldRightJoinNewFiltered1.count())

    val oldRightJoinNewFiltered  = oldRightJoinNewFiltered1.filter((oldRightJoinNewFiltered1("ovalues_value").isNotNull && oldRightJoinNewFiltered1("nvalues_value").isNotNull))

    println("oldRightJoinNewFiltered count  = " + oldRightJoinNewFiltered.count())
    val newCols = oldRightJoinNewFiltered.withColumn("changetype",lit(audit_changetypes().Response_option_added).cast(StringType))


    //get deleted responses
    val oldLeftJoinNew = dfOld.as("o").join(dfNew.as("n"),dfNew("qlabel") === dfOld("qlabel") and dfNew("values_label") === dfOld("values_label"), "left_outer" ).select(dfOld("qlabel").as("qlabel"),
      dfOld("version").as("oversion"), dfNew("version").as("nversion"), dfOld("qtitle").as("qtitle"),dfOld("values_value").cast(StringType).as("ovalues_value"),dfNew("values_value").cast(StringType).as("nvalues_value")).distinct().toDF()

    val oldLeftJoinNewFiltered1 = oldLeftJoinNew.filter(oldLeftJoinNew("nversion").isNull)
    val oldLeftJoinNewFiltered  = oldLeftJoinNewFiltered1.filter((oldLeftJoinNewFiltered1("ovalues_value").isNotNull && oldLeftJoinNewFiltered1("nvalues_value").isNotNull))

    val deletedCols = oldLeftJoinNewFiltered.withColumn("changetype",lit(audit_changetypes().Response_deleted).cast(StringType))

    /***************************************  end ***************************************************/

    //get  responses changes
    val oldInnerJoinNew = dfOld.as("o").join(dfNew.as("n"),dfNew("qlabel") === dfOld("qlabel") and dfNew("values_label") === dfOld("values_label"), "inner" ).select(dfOld("qlabel").as("qlabel"),
      dfOld("version").as("oversion"), dfNew("version").as("nversion"), dfOld("qtitle").as("qtitle"),dfOld("values_value").cast(StringType).as("ovalues_value"),dfNew("values_value").cast(StringType).as("nvalues_value")).distinct().toDF()


    val changedTextValuesFilter = oldInnerJoinNew.filter(oldInnerJoinNew("ovalues_value").notEqual(oldInnerJoinNew("nvalues_value")))
    val changedTextValues = changedTextValuesFilter.withColumn("changetype",lit(audit_changetypes().Response_text_changed).cast(StringType))
        val finalds = newCols.unionAll(deletedCols).unionAll(changedTextValues)

    finalds

  }




  def getRowChanges(dfO: DataFrame, dfN: DataFrame): DataFrame = {
    //get row records
    val dfOld = dfO.filter(dfO("row").isNotNull)
    val dfNew = dfN.filter(dfN("row").isNotNull)

    val oldRightJoinNew = dfOld.as("o").join(dfNew.as("n"),dfNew("qlabel") === dfOld("qlabel")  and dfOld("row") === dfNew("row"), "right_outer" ).select(dfNew("qlabel").as("qlabel"),dfOld("version").as("oversion"), dfNew("version").as("nversion"), dfNew("qtitle").as("qtitle"),dfNew("rowTitle").as("rowTitle")).distinct().toDF()
    val oldRightJoinNewFiltered = oldRightJoinNew.filter(oldRightJoinNew("oversion").isNull )
    val newRows = oldRightJoinNewFiltered.withColumn("changetype",lit(audit_changetypes().Row_added).cast(StringType))

    //get row deleted
    val oldLeftJoinNew = dfOld.as("o").join(dfNew.as("n"),dfNew("qlabel") === dfOld("qlabel")  and dfOld("row") === dfNew("row"), "left_outer" ).select(dfOld("qlabel").as("qlabel"),dfOld("version").as("oversion"), dfNew("version").as("nversion"), dfOld("qtitle").as("qtitle"),dfOld("rowTitle").as("rowTitle")).distinct().toDF()
    val oldLeftJoinNewFiltered = oldLeftJoinNew.filter(oldLeftJoinNew("nversion").isNull )
    val deletedRows = oldLeftJoinNewFiltered.withColumn("changetype",lit(audit_changetypes().Row_deleted).cast(StringType))

    //Hidden questions
    /***********************************************************************/


    val df_oldDFH = dfOld.filter(dfOld("rowwhere").contains("execute") || dfOld("rowwhere").contains("report")
      || dfOld("rowwhere").contains("none") || dfOld("rowwhere").contains("data") || dfOld("rowwhere").contains("summary")
      || dfOld("rowcond").contains("0") || dfOld("rowcond").contains(" 0")|| dfOld("rowcond").contains("0\"")
    )

    val df_newDFH = dfNew.filter(dfNew("rowwhere").contains("execute") || dfNew("rowwhere").contains("report")
      || dfNew("rowwhere").contains("none") || dfNew("rowwhere").contains("data") || dfNew("rowwhere").contains("summary")
      || dfNew("rowcond").contains("0") || dfNew("rowcond").contains(" 0")|| dfNew("rowcond").contains("0\"")
    )

    //New row Hidden but not present in old data
    val rightFilteredDFRaw = df_oldDFH.as("o").join(df_newDFH.as("n"),df_newDFH("qlabel") === df_oldDFH("qlabel")  and df_newDFH("row") === df_oldDFH("row"), "right_outer" ).select(dfNew("qlabel").as("qlabel"),dfOld("version").as("oversion"), dfNew("version").as("nversion"), dfNew("qtitle").as("qtitle"),dfNew("rowTitle").as("rowTitle")).distinct().toDF()

    val rightFilteredDF = rightFilteredDFRaw.filter(rightFilteredDFRaw("oversion").isNull)
    val rdf_with_type_1 = rightFilteredDF.withColumn("changetype",lit(audit_changetypes().Row_hidden).cast(StringType))


    val leftFilteredDFRaw = df_oldDFH.as("o").join(df_newDFH.as("n"),df_newDFH("qlabel") === df_oldDFH("qlabel")   and df_newDFH("row") === df_oldDFH("row"), "left_outer" ).select(dfOld("qlabel").as("qlabel"),dfOld("version").as("oversion"), dfNew("version").as("nversion"), dfOld("qtitle").as("qtitle"),dfOld("rowTitle").as("rowTitle")).distinct().toDF()
    val leftFilteredDF = leftFilteredDFRaw.filter(leftFilteredDFRaw("nversion").isNull)
    val rdf_with_type_2 = leftFilteredDF.withColumn("changetype",lit(audit_changetypes().Row_reinstated).cast(StringType))


     /***************************************  end ***************************************************/


    //reinstated questions
    val finalds = newRows.unionAll(deletedRows).unionAll(rdf_with_type_1).unionAll(rdf_with_type_2)
    finalds

  }





    def processAuditFiles(f1: String, f2: String, outputPath:String ): Unit = {

      val initialSet = StringBuilder.newBuilder
      val addToSet = (s: StringBuilder, v: String) => s.append( if(v == null) "" else v )
      val mergePartitionSets = (p1: StringBuilder, p2: StringBuilder) => p1 ++= p2

      println("1")

      val dfOldRaw = ETLConf.hiveContext.read
        .format("com.databricks.spark.xml")
        .option("rowTag", "record")
        //.option("nullValue", "-")
        .load(f1)

      println("2")

      val dfNewRaw = ETLConf.hiveContext.read
        .format("com.databricks.spark.xml")
        .option("rowTag", "record")
       // .option("nullValue", "-")
        .load(f2)
      println("3")






      val dfOld =   dfOldRaw.filter(!dfOldRaw("label").endsWith("oe") && !dfOldRaw("type").equals("text"))
      val dfNew =   dfNewRaw.filter(!dfNewRaw("label").endsWith("oe") && !dfNewRaw("type").equals("text"))


      dfOld.persist()
      dfNew.persist()


      val totlaoldrecords = dfOld.count()
      val totlanewrecords = dfNew.count()






      //Right Join
      val rightJoin = getRightJoinerQuestions(dfOld, dfNew)

      val rightJoinFilter = rightJoin.filter(rightJoin("oqlabel").isNull)
      println("10")
      val questionsAdded = rightJoinFilter.map { x =>
        val qlabel = x.getAs[String]("nqlabel")
        val questionDetails = x.getAs[String]("nqtitle")
        (( qlabel, audit_changetypes().Question_added),questionDetails)
      }

      val addedQuestions = questionsAdded.filter( x => !x._2.equals("")).aggregateByKey(initialSet)(addToSet, mergePartitionSets)
      addedQuestions.persist()

      //Left Join
      val leftJoin = getLeftJoinerQuestions(dfOld, dfNew)

      val leftJoinFilter = leftJoin.filter(leftJoin("nversion").isNull)

      val questionsDeleted = leftJoinFilter.map { x =>
        val qlabel = x.getAs[String]("oqlabel")
        val questionDetails = x.getAs[String]("oqtitle")
        (( qlabel, audit_changetypes().Question_deleted),questionDetails)
      }


      val deletedQuestions = questionsDeleted.filter( x => !x._2.equals("")).aggregateByKey(initialSet)(addToSet, mergePartitionSets)
      deletedQuestions.persist()


      //Added Questions
      var bquestions = deletedQuestions.collect().map( x => x._1._1).distinct.toList
      val finalBoradCastQuestions = bquestions ::: addedQuestions.collect().map( x => x._1._1).distinct.toList


      var lstQuestions = ListBuffer[String]()
      val broadcastVar_onlyQuestions = ETLConf.sc.broadcast(finalBoradCastQuestions)

      val colchanges = getColChanges(dfOld,dfNew)
      val colchangesDF1 = colchanges.map{ x =>
        var sb = new StringBuilder()
        val qlabel = x.getAs[String]("qlabel")
        val coltitle = x.getAs[String]("colTitle")
        val qtitle = x.getAs[String]("qtitle")
        val changetype = x.getAs[String]("changetype")
        sb.append(coltitle)
        ((qlabel, changetype), sb)
      }

      colchangesDF1.persist()

     val colchangesDF = colchangesDF1.filter( x =>  !(broadcastVar_onlyQuestions.value.contains(x._1._1) && (x._1._2.equals(audit_changetypes().Column_added) || x._1._2.equals(audit_changetypes().Column_deleted))))

       //get response changes
       val value_changes = getvalues_valueChanges(dfOld,dfNew)
       val valuechangesDF = value_changes.map{ x =>
         var sb = new StringBuilder()
         val qlabel = x.getAs[String]("qlabel")
         val ovalues_value = x.getAs[String]("ovalues_value")
         val nvalues_value = x.getAs[String]("nvalues_value")
         val changetype = x.getAs[String]("changetype")
         sb.append(s"Old Value : <$ovalues_value>  New Value : <$nvalues_value> ")
         ((qlabel, changetype), sb)
       }.filter( x =>  !(broadcastVar_onlyQuestions.value.contains(x._1._1) && (x._1._2.equals(audit_changetypes().Response_option_added) || x._1._2.equals(audit_changetypes().Response_deleted))))


       valuechangesDF.persist()

      //get row changes
      val rowchanges = getRowChanges(dfOld,dfNew)
      val rowchangesDF = rowchanges.map{ x =>
        var sb = new StringBuilder()
        val qlabel = x.getAs[String]("qlabel")
        val rowtitle = x.getAs[String]("rowTitle")
        val qtitle = x.getAs[String]("qtitle")
        val changetype = x.getAs[String]("changetype")
        sb.append(rowtitle)
        ((qlabel, changetype), sb)
      }.filter( x =>  !(broadcastVar_onlyQuestions.value.contains(x._1._1) && (x._1._2.equals(audit_changetypes().Row_added) || x._1._2.equals(audit_changetypes().Row_deleted))))

      rowchangesDF.persist()      ///Hidden Questions
      //If any “where” attribute contains execute, report, none, data, summary then hidden
      //If any “cond” attribute = 0, or contains “and 0” followed by a space or “, then hidden

      val hidQuestions = getHiddenQuestions(dfOld,dfNew)

      val hidQuestionsChange = hidQuestions.map{ x =>
        var sb = new StringBuilder()
        val oqlabel = x.getAs[String]("oqlabel")
        val nqlabel = x.getAs[String]("nqlabel")
        val hidtype = x.getAs[String]("hidtype")
        val oqtitle = x.getAs[String]("oqtitle")
        val nqtitle = x.getAs[String]("nqtitle")
        if(hidtype.equals(audit_changetypes().Question_reinstated)) {
          sb.append(oqtitle)
          ((oqlabel, audit_changetypes().Question_reinstated), sb)
        }
        else {
          sb.append(nqtitle)
          ((nqlabel, audit_changetypes().Question_hidden), sb)
        }
      }
      //Inner Join
      val innerJoin = getInnerJoinerQuestions(dfOld, dfNew)
      ///Question level changes
      val innerJoinTypeDiffs = innerJoin.filter(!innerJoin("oqtype").equalTo(innerJoin("nqtype")) )
      //innerJoinTypeDiffs.show(100)

      val questionLeveltypeChange1 = innerJoinTypeDiffs.map{ x =>
        var sb = new StringBuilder()
        val oqlabel = x.getAs[String]("oqlabel")
        val oldtype = x.getAs[String]("oqtype")
        val newtype = x.getAs[String]("nqtype")
        if(!newtype.equals("text")   ) {
          var retValue = comparetexts(oldtype, newtype, "Question Type")
          if (!retValue.equals("")) sb.append(retValue)
          ((oqlabel, audit_changetypes().Question_type_changed), sb.toString)
        }
        else {
          ((oqlabel, audit_changetypes().Question_type_changed), "")
        }
      }.filter( x => !x._2.isEmpty)


      val questionLeveltypeChange = questionLeveltypeChange1.filter( x => !x._2.equals(""))
      val typeChagedQuestions = questionLeveltypeChange.filter( x => !x._2.equals("")).aggregateByKey(initialSet)(addToSet, mergePartitionSets)
      typeChagedQuestions.persist()


      val questionLevelChanges = innerJoin.map{ x =>
        var sb = StringBuilder.newBuilder
        val row = questionAttrs(x.getString(0),x.getString(1),x.getString(2), x.getString(3), x.getString(4), x.getString(5), x.getString(6), x.getString(7), x.getString(8), x.getString(9), x.getString(10), x.getString(11), x.getString(12))
        var retValue = comparetexts(row.oqtitle, row.nqtitle, "Title")
        if(!retValue.equals("") ) sb.append(retValue)
        (( row.oqlabel, audit_changetypes().Question_text_changed ), sb.toString)
      }



      val finalQuestionLevelChanges = questionLevelChanges.filter( x => !x._2.equals("")).aggregateByKey(initialSet)(addToSet, mergePartitionSets)
      finalQuestionLevelChanges.persist()
      val finalSet = finalQuestionLevelChanges.union(addedQuestions).union(deletedQuestions).union(typeChagedQuestions).union(hidQuestionsChange).union(rowchangesDF).union(colchangesDF).union(valuechangesDF)
      val fds = finalSet.map { x =>
        x.productIterator.mkString(",")
      }

      //finalSet.saveAsTextFile(outputPath)
      fds.coalesce(1).saveAsTextFile(outputPath)
    }

    def getVersionNumber(filename : String) : String ={
      var retValue : String = ""
      val dot = filename.lastIndexOf(".")
      val under = filename.lastIndexOf("_") + 2


      if(dot == under)
        retValue = "1"
      else
        retValue = filename.substring(under, dot)

      retValue
    }

    //Working code
    def main(args: Array[String]): Unit = {

      //c:\ravi\data\ab_datamap_bsb14022_v27.xml
   //   processAuditFiles("c:\\ravi\\data\\ab_datamap_bsb14022_v27.xml", "c:\\ravi\\data\\ab_datamap_bsb14022_v41.xml", "")  //ToDO delete this line
     /* val f1 = "C:\\Ravi\\Source\\Primary_Research_Strategic_Data\\primary_research_karthik\\newdata\\ab_datamap_bsb14022_v26.xml"
      val f2 = "C:\\Ravi\\Source\\Primary_Research_Strategic_Data\\primary_research_karthik\\newdata\\ab_datamap_bsb14022_v27.xml"
      processAuditFiles(f1,f2,"")*/


      val config: Config = ConfigFactory.load("application")
      val fsConfig = HDFSConfForFileSystem.getConfForHDFSFileSystem(config)
      val hm = new hdfsFileManager(fsConfig)
      val hdfsSurveysXmlAuditLogPath = config.getString("app.directory.hdfsSurveysXmlAuditLogPath")
      val publishedXMLBaseOutputPath = config.getString("app.directory.hdfsPublishedParsedXMLOutputPath")

      val hdfsSurveysXmlAuditOutputPathFormat = config.getString("app.directory.hdfsSurveysXmlAuditOutputPath")  + "%s/"
      val surveys_to_process = config.getStringList("app.surveyidsForXMLParsing.names").toList
      if (surveys_to_process.length > 0) {
        val filesToProcess = hm.getParsedSurveyXMLFilePathsRecursively(publishedXMLBaseOutputPath, surveys_to_process)
        filesToProcess.foreach(x => {
          val surveyid = x._1
         val versionsToVerify = x._2.map(filename => {

            val dot = filename.lastIndexOf(".")

            val under = filename.lastIndexOf("_") + 2
            if(dot == under)
              (1, filename)
            else
              (filename.substring(under, dot).toInt, filename)
          }).toList


          var end_processing : Boolean = false

          if (versionsToVerify.length < 2)
            end_processing = true

          while(!end_processing) {
            val existingEntries = getLatestAuditEntries(config, hdfsSurveysXmlAuditLogPath)
            //When no entries are found then select sorted files one and two
            if (existingEntries.length <= 0 && versionsToVerify.length >= 2) {

              val oldFileDetails = versionsToVerify.sortWith(_._1 < _._1)(0)
              val newFileDetails = versionsToVerify.sortWith(_._1 < _._1)(1)

              val surveyFolder = hdfsSurveysXmlAuditOutputPathFormat.format(surveyid)
              if (hm.IsFolderExists(surveyFolder, true)) {
                println("survey id folder created successfully")
              }

              val newfileName = surveyFolder + "lb_audit_%s_v%s_v%s".format(surveyid, oldFileDetails._1, newFileDetails._1)

              processAuditFiles(oldFileDetails._2, newFileDetails._2, newfileName)
              val existingLogEntries = new ArrayBuffer[audit_logInfo]()
              val newlog = audit_logInfo("", newfileName, oldFileDetails._1.toString, newFileDetails._1.toString, surveyid)
              existingLogEntries.append(newlog)

              hm.writeAuditLogEntries(hdfsSurveysXmlAuditLogPath, existingLogEntries)
            }
            else  {

              if(existingEntries.filter(y => y.surveryid == surveyid).length <= 0) {
                println("ending the process due to no existingEntries^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")
                end_processing = true
                }
              else {

                val newfileToAuditDetails1 = versionsToVerify.filter(x => (x._1 > existingEntries.filter(y => y.surveryid == surveyid).head.new_vno.toInt)).sortWith(_._1 < _._1)
                val lastFileAuditedEntry = existingEntries.filter(y => y.surveryid == surveyid)
                if (lastFileAuditedEntry.length <= 0 || newfileToAuditDetails1.length <= 0) {
                  end_processing = true
                }
                else {
                  val lastFileAudited = lastFileAuditedEntry.head
                  val newfileToAuditDetails = newfileToAuditDetails1.head
                  if (lastFileAudited.new_vno.equals(newfileToAuditDetails._1)) {
                    end_processing = true
                  }
                  else {

                    val surveyFolder = hdfsSurveysXmlAuditOutputPathFormat.format(surveyid)
                    if (hm.IsFolderExists(surveyFolder, true)) {
                      println("survey id folder created successfully")
                    }

                    val newfileName = surveyFolder + "lb_audit_%s_v%s_v%s".format(surveyid, lastFileAudited.new_vno, newfileToAuditDetails._1)

                    processAuditFiles(lastFileAudited.filePath, newfileToAuditDetails._2, newfileName)

                    val existingLogEntries = new ArrayBuffer[audit_logInfo]()
                    val newlog = audit_logInfo("", newfileName, lastFileAudited.new_vno.toString, newfileToAuditDetails._1.toString, surveyid)
                    existingLogEntries.append(newlog)

                    hm.writeAuditLogEntries(hdfsSurveysXmlAuditLogPath, existingLogEntries)
                  }
                }
              }
            }
          }
        })
      }
    }
}
