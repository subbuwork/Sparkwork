package com.apache.spark.twitter

import com.apache.spark.utils.TUtils
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.SQLContext

object SentimentAnalysis extends TUtils {
  
  System.setProperty("twitter4j.oauth.consumerKey", "provide your credentials")
  System.setProperty("twitter4j.oauth.consumerSecret", "")
  System.setProperty("twitter4j.oauth.accessToken", "")
  System.setProperty("twitter4j.oauth.accessTokenSecret", "")
  
  //val sqlContext  = new SQLContext(sc)
  
  val tStreaming = TwitterUtils.createStream(ssc,None)
  
  val tags = tStreaming.filter { t =>
                                val tags = t.getText.split(" ").filter(_.startsWith("#")).map(_.toLowerCase())
                                tags.contains("bigdata") && tags.contains("food")
  
  }
  
  tags.count()
  
 
  
}