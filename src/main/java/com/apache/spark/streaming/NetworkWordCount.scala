package com.apache.spark.streaming

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming._
import org.apache.spark.streaming.Seconds
import org.apache.spark.SparkConf

object NetworkWordCount  {
  def main(args: Array[String]): Unit = {
    
  
  val conf = new SparkConf().setAppName("NetworkWordCount").setMaster("local[2]")
  
  val ssc = new StreamingContext(conf,Seconds(2))
  
  val lines = ssc.socketTextStream("localhost",9999)
  
  val words = lines.flatMap(_.split(" "))
  val count = words.map(w=>(w,1)).reduceByKey(_+_)
  count.print()
  ssc.start()
  ssc.awaitTermination()
  
  }
  
}