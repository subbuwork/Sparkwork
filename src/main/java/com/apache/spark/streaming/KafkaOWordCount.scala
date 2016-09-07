package com.apache.spark.streaming

import kafka.serializer.StringDecoder
import com.apache.spark.utils.KUtils

import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._

object KafkaOWordCount extends KUtils{
  
  /*val brokers = Array("localhost:9092")
  val topics  = Set("test")
  */
  
   if (args.length < 2) {
      System.err.println(s"""
        |Usage: KafkaOWordCount <brokers> <topics>
        |  <brokers> is a list of one or more Kafka brokers
        |  <topics> is a list of one or more kafka topics to consume from
        |
        """.stripMargin)
      System.exit(1)
    }
  val (zkQuorum,group,topics,numThreads) = ("localhost:2181","testgp","testtopic",2)
  val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
  //val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)     
  // Get the lines, split them into words, count the words and print
                
  val words = lines.flatMap(line =>line.split(" "))
  val wordCount = words.map(word=>(word,1L)).reduceByKey(_+_)
  wordCount.print()
  
  ssc.start()
  ssc.awaitTermination()
     
  
  
}