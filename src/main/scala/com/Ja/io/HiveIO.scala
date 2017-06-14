package com.Ja.io

import breeze.linalg.*
import com.Ja.conf.ETLConf._
import org.apache.spark.sql.DataFrame
import com.Ja.tables.Table

/**
  * Created by zhenhao.li on 01/11/2016.
  */

/*
trait HiveSchema{
    def equals(schema: HiveSchema): Boolean
    def isSubSetOf(schema: HiveSchema): Boolean
    def isSuperSetOf(schema: HiveSchema): Boolean
}


trait HiveIO{
    def readTable(url: String): HiveTable
    def writeTable(HiveTable: DataFrame, schema: HiveSchema, url: String): Unit
    def readSchema(url: String): HiveSchema
}

class HiveTable(hiveSchema: HiveSchema){
    def toDataFrame: DataFrame = ???
}

*/


object HiveIO{

    //TODO solve the small file problem
    //TODO handle table partitioning
    /**
      * @param table is the internal Table for the atomic/logic/mart block
      * @param path is the HDFS path of the avro files
      * @param tableName is the name of the Hive table to be created
      * */
    def createTable(table: Table, path: String, tableName: String) ={

        table.getDF.registerTempTable("tempTable")

        val rawSchema = table.getSchema

        
        /**
          * Be aware of the type translation from Spark Dataframe to Hive.
          * Hive use int instead of integer,
          * and bigint instead of long.
          * Field name cannot start with "_".
          * */
        val schemaString = rawSchema.fields.map(field => field.name.replaceAll("""^_""", "").concat(" ").concat(field.dataType.typeName match {
            case "integer" => "int"
            case "long" => "bigint"
            case smt => smt
        })).mkString(",\n")

        /*hiveContext.sql(
            s"""
               |Create external table IF NOT EXISTS $tableName ($schemaString) \n
               |Stored As Avro \n
               |-- inputformat 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat' \n
               |-- outputformat 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat' \n
               |Location '$path'
       """.stripMargin
        )*/

        /*hiveContext.sql(
            s"""
               |INSERT OVERWRITE TABLE $tableName SELECT * FROM tempTable
       """.stripMargin
        )*/

        hiveContext.dropTempTable("tempTable")

    }



}

//TODO Delete Me

object HiveIOWithoutSchema{

    def writeToHive(tempTableName: String, hiveTableName: String) = {
        hiveContext.sql(
            s"""create table $hiveTableName
                |as select * from $tempTableName
             """.stripMargin
        )
            //"create table " + hiveTableName + " as select * from " + tempTableName)
    }


    def writeToHiveWithPath(tempTableName: String, hiveTableName: String, path: String) = {
        hiveContext.sql(
            s"""create table $hiveTableName
                |USING com.databricks.spark.avro
                |OPTIONS ( path "$path/data.avro" )
                |as select * from $tempTableName
             """.stripMargin
        )
        //"create table " + hiveTableName + " as select * from " + tempTableName)
    }

}




/*
*
val rawSchema = sqlContext.read.avro("Path").schema
val schemaString = rawSchema.fields.map(field => field.name.replaceAll("""^_""", "").concat(" ").concat(field.dataType.typeName match {
        case "integer" => "int"
        case smt => smt
      })).mkString(",\n")

val ddl =
      s"""
         |Create external table $tablename ($schemaString) \n
         |partitioned by (y int, m int, d int, hh int, mm int) \n
         |Stored As Avro \n
         |-- inputformat 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat' \n
         | -- outputformat 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat' \n
         | Location 'hdfs://$path'
       """.stripMargin



        Create external table $tableName ($schemaString) \n
Location 'file:///$path'
STORED AS AVRO
*/




