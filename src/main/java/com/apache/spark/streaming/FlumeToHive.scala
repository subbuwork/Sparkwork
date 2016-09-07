package com.apache.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.SaveMode

object FlumeToHive {
  
  val splits = "([^ ]*) ([^ ]*) ([^ ]*) (-|\\[[^\\]]*\\]) ([^ \"]*|\"[^\"]*\") (-|[0-9]*) (-|[0-9]*) ([^ \"]*|\"[^\"]*\") (.*)".r
  
  case class apachelog(host:String,identity:String,user:String,time:String,request:String,status :String,size:String,url:String,system_Info:String)
  
  
  
  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf().setAppName(getClass.getSimpleName).setMaster("local[2]")
    val ssc  = new StreamingContext(conf,Seconds(5))
    
    val hv = new HiveContext(ssc.sparkContext)
    
    import hv.implicits._
    hv.setConf("hive.metastore.uris","thrift://localhost:9083")
    hv.sql("use hivedb")
    
    val stream = FlumeUtils.createPollingStream(ssc,"localhost",6666, StorageLevel.MEMORY_ONLY_SER_2)
    stream.foreachRDD{rdd =>
      
      val data = rdd.map(line=>line.event)
      
      val rddData = data.map(d=>new String(d.getBody.array()))
      val table   = rddData.map(line=>createColumns(line)).map(c=>apachelog(c(0), c(1), c(2),c(3), c(4), c(5), c(6), c(7), c(8)))
      
      val fTable = table.toDF()
      if(fTable.count()>0)fTable.show()
      fTable.write.format("orc").mode(SaveMode.Append).saveAsTable("flumnetable")
      
      
    }
    ssc.start()
    ssc.awaitTermination() 
   }
  def createColumns(line:String):Array[String]=  {
    
    var splits(host,identity,user,time,request,status,size,url,system_Info) = line
    Array(host,identity,user,time,request,status,size,url,system_Info)
  
  
    
  }
}