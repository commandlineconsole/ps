package com.sky.tables

import com.sky.conf.ETLConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

/**
  * Created by zhenhao.li on 03/11/2016.
  */


/**
  * Table is the abstraction underlying all atomic blocks, logic blocks, and data marts.
  * A table has a schema and a dataframe.
  * The constructor is private so that the only way to create an object of Table is to use the apply method in the companion object of Table.
  * @param schema is the schema of the table to be constructed
  * @param df is the dataframe that essentially is the table
  */
class Table private(schema : StructType, df: DataFrame) {
    def getDF: DataFrame = df
    def getSchema: StructType = schema
}

/**
  * The purpose of this companion object of Table is to enforce schema validation when creating objects of class Table.
  * Note that the order of rows in a input dataframe may not be preserved in the output, because we apply sc.union on RDDs.
  */
object Table {
    /**
      * The apply method is essentially the only way to create an object of Table.
      * It takes a sequence of dataframe instead of one. That is so for two reasons:
      * 1. to make it different from the constructor of the Table class which takes only one dataframe,
      * 2. to make is possible to union multiple dataframes into one in a safe way.
      */
    def apply(givenSchema : StructType, dfs: DataFrame *) = {
        import com.sky.conf.ETLConf._
        val rdds = dfs.map(df => df.rdd)
        val unionedRDD = try {
            hiveContext.createDataFrame(sc.union(rdds), givenSchema)
        } catch {
            case e: Throwable => log.error("cannot create the AtomicBlock Dataframe. \n Required schema is " + givenSchema.toString)
                throw (e)
        }
        new Table(givenSchema, unionedRDD)
    }
}
