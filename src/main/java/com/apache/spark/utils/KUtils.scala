package com.apache.spark.utils

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.sql.SQLContext


trait KUtils extends App{
  
  val master = "local"
  val cassandraHost = "localhost"
  
  val conf = new SparkConf(true).
             setAppName(getClass.getSimpleName)
             .setMaster(master)
             .set("spark.cassandra.connection.host",cassandraHost)
  val ssc  = new StreamingContext(conf,Seconds(20)) 
  //val sqlContext = new SQLContext(ssc.sparkContext)
}
object KUtils{
  def apply():KUtils = new KUtils{}
}