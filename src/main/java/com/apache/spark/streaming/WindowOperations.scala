package com.apache.spark.streaming

import org.apache.spark.SparkConf
import org.datanucleus.query.evaluator.memory.GetClassMethodEvaluator
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.storage.StorageLevel

object WindowOperations {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(getClass().getSimpleName).setMaster("local[2]")
    val ssc = new StreamingContext(conf,Seconds(1))
    val lines = ssc.socketTextStream("localhost",9999)
    
    val words = lines.flatMap(_.split(" "))
    val paris = words.map(w=>(w,1))
    val count = paris.reduceByKeyAndWindow((a:Int,b:Int)=>a+b,Seconds(30),Seconds(10))
    count.print()
    ssc.start()
    ssc.awaitTermination()
    
    
  }
}