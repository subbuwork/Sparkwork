package com.apache.spark.utils

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds

trait CsvParsingUtil extends App{
  val master = "local[*]"
  
  val conf = new SparkConf(true).setAppName(getClass.getSimpleName)
  val sc   = new SparkContext(conf)
  val ssc  = new StreamingContext(sc,Seconds(5))
  
  
  
  
}