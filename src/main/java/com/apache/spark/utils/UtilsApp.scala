package com.apache.spark.utils

import org.apache.spark.{SparkConf,SparkContext}
trait UtilsApp extends App {
  val sparkMasterHost = "127.0.0.1"
  val cassandraHost = "localhost"
  
  val words = "/home/spark/Documents/inputdata/words.txt"
  
  // Tell Spark the address of one Cassandra node:
  val conf = new SparkConf(true)
                 .setAppName(getClass.getSimpleName)
                 .setMaster("local")
                 .set("spark.cassandra.connection.host",cassandraHost)
                 .set("spark.cleaner.ttl","3600")
  
  // Connect to the Spark cluster:
  lazy val sc = new SparkContext(conf)               
  
}
object UtilsApp{
  def apply():UtilsApp = new UtilsApp{}
}