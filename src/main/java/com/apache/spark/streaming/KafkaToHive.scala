package com.apache.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.log4j.{Logger,Level}
import org.apache.spark.SparkContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.sql.SaveMode

object KafkaToHive {
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  
  val splits = "([^ ]*) ([^ ]*) ([^ ]*) (-|\\[[^\\]]*\\]) ([^ \"]*|\"[^\"]*\") (-|[0-9]*) (-|[0-9]*) ([^ \"]*|\"[^\"]*\") (.*)".r
  
  case class apachelog(host:String,identity:String,user:String,time:String,request:String,status :String,size:String,url:String,system_Info:String)
  
  
  def main(args: Array[String]): Unit = {
    
    val (zkQuorum,group,topics,numThreads) = ("localhost:2181","testgp","testtopic",2)
    
    val conf = new SparkConf(true).setAppName(getClass.getSimpleName).setMaster("local[4]")
    val sc   = new SparkContext(conf)
    val ssc = new StreamingContext(sc,Seconds(5))
    val hive = new HiveContext(ssc.sparkContext)
    hive.setConf("hive.metastore.uris","thrift://localhost:9083")
    import hive.implicits._
    hive.sql("use hivedb")
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
    lines.foreachRDD{rdd=>
    val rdd1 = rdd.map(line=>createColumns(line)).map(c=>apachelog(c(0), c(1), c(2),c(3), c(4), c(5), c(6), c(7), c(8)))
    val table = rdd1.toDF()
    table.show
    table.write.mode(SaveMode.Append).saveAsTable("kafkatreaming1") 
    println("Job completed!!!")
    }
    
  ssc.start()
  ssc.awaitTermination()
  
  }
  def createColumns(line:String):Array[String]=  {
    var splits(host,identity,user,time,request,status,size,url,system_Info) = line
    Array(host,identity,user,time,request,status,size,url,system_Info)  
  }
  
  
  
}