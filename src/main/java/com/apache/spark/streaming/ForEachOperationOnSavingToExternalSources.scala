package com.apache.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.storage.StorageLevel
import javax.sql.ConnectionPoolDataSource

object ForEachOperationOnSavingToExternalSources {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(getClass().getSimpleName).setMaster("local[2]")
    val ssc  = new StreamingContext(conf,Seconds(1))
    
    val lines = ssc.socketTextStream("localhost",9999,StorageLevel.MEMORY_AND_DISK_2)
    
    /*lines.foreachRDD{rdd=>
      rdd.foreachPartition { partitionOfRecords =>
        // ConnectionPool is a static, lazily initialized pool of connections
    //val connection = ConnectionPool.getConnection()
      partitionOfRecords.foreach(record =>connection.send(record))
      
      }
      ConnectionPool.returnConnection(connection)// return to the pool for future reuse
      
      *//**
       * Note that the connections in the pool should be lazily created on demand 
       * and timed out if not used for a while. This achieves the most efficient 
       * sending of data to external systems.
       */
      
   // }
    
    
  }
}