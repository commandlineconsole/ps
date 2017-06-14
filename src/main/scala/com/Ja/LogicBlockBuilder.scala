/*
package com.sky

import com.sky.conf.JobConfig
import com.sky.io.{HiveIOWithoutSchema, IOUtil, JsonIO}
import com.sky.conf.ETLConf._
import com.sky.tables.atomic.AtomicBlockDatamap
/**
  * Created by zhenhao.li on 25/11/2016.
  */

//TODO rewrite this part with the tables.logic package/module

object LogicBlockBuilder {
    import com.sky.common.DataFrameFunctions._
    import hiveContext.implicits._

    def main(args: Array[String]): Unit = {
        val jobConfig = new JobConfig(args(0))

        import jobConfig._

        val mappingData = IOUtil.readMappingData(mappingTSVpath).get
        println("mapping data is ...")
        mappingData.printSchema()
        mappingData.show()

        val mappingDataToJoin = mappingData.select("Survey_Name", "Question_Label", "Meta_Label")


        /**     Read Hive table */

        val datamapAtomic = hiveContext.sql("FROM test_datamap_atomic_block SELECT * ")


        /**     Join Datamap with Mapping table table */

        val dataMapLogic = datamapAtomic.join(mappingDataToJoin, datamapAtomic("source") <=> mappingDataToJoin("Survey_Name") && datamapAtomic("qlabel") <=> mappingDataToJoin("Question_Label"), "left_outer").drop("Question_Label").drop("Survey_Name")

        /**     Write Hive table */

        hiveContext.sql("DROP TABLE IF EXISTS test_datamap_logic_block")

        dataMapLogic.registerTempTable("datamapLogic")
        HiveIOWithoutSchema.writeToHive("datamapLogic", "test_datamap_logic_block")


    }

}
*/
