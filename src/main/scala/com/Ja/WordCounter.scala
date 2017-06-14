package com.Ja

/**
  * Created by zhenhao.li on 01/11/2016.
  */

//TODO DELETE ME
/**
  * This is not part of any sky project. It is here to show how the testing framework works.
  */

import org.apache.spark.rdd.RDD

object WordCounter {
    def count(lines: RDD[String]): RDD[(String, Int)] = {
        val wordsCount = lines.flatMap(l => l.split("\\W+"))
          .map(word => (word, 1))
          .reduceByKey(_ + _)
        wordsCount
    }
}

