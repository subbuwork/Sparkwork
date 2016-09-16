package com.apache.spark.twitter

import com.apache.spark.utils.TUtils
import org.apache.spark.streaming.twitter.TwitterUtils

object TwitterAnalysis extends TUtils{
  
    /*System.setProperty("twitter4j.oauth.consumerKey", "provide your credentials")
  System.setProperty("twitter4j.oauth.consumerSecret", "")
  System.setProperty("twitter4j.oauth.accessToken", "")
  System.setProperty("twitter4j.oauth.accessTokenSecret", "")*/
  
  val tStreaming = TwitterUtils.createStream(ssc,None)
  
  val tags = tStreaming.flatMap{status=> status.getHashtagEntities.map(_.getText)
  
  }
  
  tags.countByValue().foreachRDD{rdd=>
    
  val now = org.joda.time.DateTime.now()
  
            rdd.sortBy(_._2).map(x=>(x,now)).saveAsTextFile("/home/spark/Documents/inputdata/twitter.txt")
  }
 
  ssc.start()
  ssc.awaitTermination()
}