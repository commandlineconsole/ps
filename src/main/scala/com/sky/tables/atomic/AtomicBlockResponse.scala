package com.sky.tables.atomic

import com.sky.tables.{Table}
import org.apache.spark.sql.DataFrame
import AtomicBlockSchemas._

/**
  * Created by zhenhao.li on 26/11/2016.
  */


/**
  * Use new AtomicBlockResponse(df1, df2, ...) to create a the atomic block for Response
  */
class AtomicBlockResponse private(table : Table) {

    def getTable = table
    def this(dfs: DataFrame *) = this(Table(responseSchema, dfs:_*))

}
