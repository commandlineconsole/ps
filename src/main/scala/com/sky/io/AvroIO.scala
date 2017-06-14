package com.sky.io
import org.apache.spark.sql.DataFrame

/**
  * Created by zhenhao.li on 01/11/2016.
  */

trait AvroIO {
    def read(url: String): DataFrame
    def write(df: DataFrame, url: String): Unit
}
