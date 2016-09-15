package com.apache.spark.streaming.poc

import org.apache.ivy.core.module.descriptor.ExtendsDescriptor
import com.apache.spark.utils.KUtils
import org.apache.log4j.{Logger,Level}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.Row

object PipelineDemo extends KUtils{
  
  /*
   * This is demo class of streaming pipeline which ingest data 
   * from JMeter(In real time we use WebServices instead of JMeter)==>Active-MQ==>Flume
   * ==>Kafka==>SparkStreaming(Application,not service)==>Hive table.
   * For this, entire process we have to configure and up and running below listed services.
   * 
   * JMeter //Source 
   * Active-MQ //Broker(User either Queue(one to one) or Topic(one to many))
   * Flume
   * Kafka
   * HDFS
   * Hive metastore
   * Hive(Optional) 
   * 
   */
   Logger.getLogger("org").setLevel(Level.OFF)
   Logger.getLogger("akka").setLevel(Level.OFF)

    val (zkQuorum,group,topics,numThreads) = ("localhost:2181","testgp","ebay",2)
    case class ebay(auctionId:String,bid:String,bidTime:String,bidder:String,
                    bidderRate:String,openBid:String,price:String,item:String,days:String)
    
    val hive = new HiveContext(ssc.sparkContext)
    hive.setConf("hive.metastore.uris","thrift://localhost:9083")
    import hive.implicits._
    hive.sql("use hivedb")
  
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
    lines.foreach(line=>line.collect())
    lines.foreachRDD{rdd=>
      
    val data= rdd.map(line=>line.split(",")).map(col=>ebay(col(0),col(1),col(2),col(3),col(4),col(5),col(6),col(7),col(8)))
    
    val df = data.toDF()
    df.show()
    
    df.write.mode(SaveMode.Append).saveAsTable("ebay_csv")
    print("Job completed!!!!")
    }
    
  ssc.start()
  ssc.awaitTermination()
  
  
}