package com.sky.tables.atomic


import com.sky.tables._
import org.apache.spark.sql.{DataFrame, SaveMode}
import AtomicBlockSchemas._
import com.databricks.spark.avro._

/**
  * Created by zhenhao.li on 02/11/2016.
  */

/**
  * Use new AtomicBlockDataMap(df1, df2, ...) to create a the atomic block for Datamap
  */

class AtomicBlockDatamap private(table : Table) {
    def getTable = table
    def this(dfs: DataFrame *) = this(Table(datamapSchema, dfs:_*))

}




