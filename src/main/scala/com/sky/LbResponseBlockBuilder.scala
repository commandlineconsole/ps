package com.sky

import java.io.{BufferedReader, IOException, PrintWriter}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import java.text.SimpleDateFormat
import java.util.Calendar

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.security.UserGroupInformation

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

//import com.sky.common.hdfsFileManager
import com.sky.conf.{ETLConf, JobConfig}
import org.apache.hadoop.fs.{FileSystem, Path}
import scala.collection.mutable.HashMap
import scala.collection.mutable.ArrayBuffer
import com.sky.domain._

/**
  * Created by RCH28 on 02/05/2017.
  */
class LbResponseBlockBuilder {


  def main(args: Array[String]): Unit = {
      ETLConf
  }
}
