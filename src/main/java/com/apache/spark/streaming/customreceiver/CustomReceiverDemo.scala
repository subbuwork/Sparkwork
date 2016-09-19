package com.apache.spark.streaming.customreceiver

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds

object CustomReceiverDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(getClass().getSimpleName).setMaster("local[*]")
    val ssc  = new StreamingContext(conf,Seconds(10))
    
    val receiverData = ssc.receiverStream(new CustomReceiver("localhost",9999))
    val words = receiverData.flatMap(line=>line.split(" "))
    val wordsCount = words.map(word=>(word,1)).reduceByKey(_+_)
    wordsCount.print()
    ssc.start()
    ssc.awaitTermination()
    
  }
}