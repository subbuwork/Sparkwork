package com.apache.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object ReadAndWriteJsonFile {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ReadAndWriteJsonFile").setMaster("local")
    val sc   = new SparkContext(conf)
    val sql  = new SQLContext(sc)
    val jsonData = sql.read.json("/home/spark/Documents/emp.json")
    
    jsonData.show()
  }
}