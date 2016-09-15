package com.apache.spark.streaming

import com.apache.spark.utils.KUtils
import org.apache.spark.streaming.kafka.KafkaUtils
import com.datastax.spark.connector.cql.CassandraConnector

object Flume_Kafka_Cassandra extends KUtils{
  /*
   * This class ingest data from flume to kafka to cassandra using spark
   */
  
  val splits = "([^ ]*) ([^ ]*) ([^ ]*) (-|\\[[^\\]]*\\]) ([^ \"]*|\"[^\"]*\") (-|[0-9]*) (-|[0-9]*) ([^ \"]*|\"[^\"]*\") (.*)".r
  
  case class apachelog(host:String,identity:String,user:String,time:String,request:String,status :String,
                       size:String,url:String,system_Info:String)
  val (zkQuorum,group,topics,numThreads) = ("localhost:2181","testgp","cassandra",2)
  val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
  val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
  lines.foreachRDD{rdd=>
    println("inside foreachRDD!!!!!")
    val rdd1 = rdd.map(line=>createColumns(line)).map(word=>apachelog(word(0), word(1), word(2),word(3), word(4),word(5), word(6),word(7), word(8)))
 
    import com.datastax.spark.connector._
    rdd1.saveToCassandra("test1", "loganalsys")
    
    println("Job comepleted!!!!!")
  }
  ssc.start()
  ssc.awaitTermination()
  
   def createColumns(line:String):Array[String]=  {
    println("createColumns!!!!!!!!!!")
    var splits(host,identity,user,time,request,status,size,url,system_Info) = line
    Array(host,identity,user,time,request,status,size,url,system_Info)
  
  
    
  }
}