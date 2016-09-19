package com.apache.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.apache.spark.streaming.Seconds
import org.apache.spark.storage.StorageLevel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

object DataFrameAndSqlOperationsOnDStream {
  /** Case class for converting RDD to DataFrame */
  case class Record(word:String)
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf(true).setAppName(getClass().getSimpleName).setMaster("local[2]")
    val ssc  = new StreamingContext(conf,Seconds(5))
    
    val lines = ssc.socketTextStream("localhost",9999,StorageLevel.MEMORY_AND_DISK_2)
    val words =lines.flatMap(line=>line.split(" "))
    
    words.foreachRDD{(rdd:RDD[String],time:Time)=>
    val sqlCtx = new SQLContext(rdd.sparkContext)
    
    import sqlCtx.implicits._
    
    // Convert RDD[String] to RDD[case class] to DataFrame
    val wordDF = rdd.map(w=>Record(w)).toDF()
    
    wordDF.registerTempTable("words")
    // Do word count on table using SQL and print it
    val wordCount = sqlCtx.sql("select word,count(1) from words group by word")
    println(s"========= $time =========")
    wordCount.show()
    
    }
    ssc.start()
    ssc.awaitTermination()
    
  }
}