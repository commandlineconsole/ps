package com.sky.tables.logic

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import com.sky.tables._
/**
  * Created by zhenhao.li on 02/11/2016.
  */
/*

class Table2 private(df: DataFrame, schema: StructType) extends Table(df, schema)

object Table2 extends TableConstructor[Table2]{
    val schema = StructType(List(StructField("name", StringType), StructField("age", IntegerType)))
    def apply(df: DataFrame) = {
        //TODO
        /** validate schema; throw error when invalid */
        //def createT(df: DataFrame, schema: StructType): T = new T(df, schema)
        new Table2(df, schema)
    }
}*/
