package com.apache.spark.utils

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds


trait KUtils extends App{
  
  val master = "local[*]"
  
  val conf = new SparkConf(true).
             setAppName(getClass.getSimpleName)
             .setMaster(master)
  val ssc  = new StreamingContext(conf,Seconds(15)) 
}
object KUtils{
  def apply():KUtils = new KUtils{}
}