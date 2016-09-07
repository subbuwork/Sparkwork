package com.apache.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object ReadingWritingAvroFile {
  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf().setAppName("ReadingWritingAvroFile").setMaster("local")
    val sc   = new SparkContext(conf)
    val hv  = new SQLContext(sc)
    val avroData = hv.read.format("com.databricks.spark.avro").load("/home/spark/Documents/twitter.avro")
    avroData.printSchema()
    avroData.show()
    avroData.registerTempTable("twitter")
    hv.sql("select * from twitter").show()
    
    
  }
}