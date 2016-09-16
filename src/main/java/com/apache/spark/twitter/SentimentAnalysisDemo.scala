package com.apache.spark.twitter

import com.apache.spark.utils.TUtils
import org.apache.spark.streaming.twitter.TwitterUtils

object SentimentAnalysisDemo extends TUtils {
  
  /*System.setProperty("twitter4j.oauth.consumerKey", "provide your credentials")
  System.setProperty("twitter4j.oauth.consumerSecret", "")
  System.setProperty("twitter4j.oauth.accessToken", "")
  System.setProperty("twitter4j.oauth.accessTokenSecret", "")*/
  
  
  val tStreaming = TwitterUtils.createStream(ssc,None)
  
  val tags = tStreaming.filter{f=>
                val tags = f.getText.split(" ").filter(_.startsWith("#")).map(_.toLowerCase())
               tags.contains("bigdata") | tags.contains("jewllery")
               
  
  }
  tags.print(10)
  /*val hashTags =tStreaming.flatMap(status=>status.getHashtagEntities)
  val hashPairs = hashTags.map(tag=>(tag.getText.startsWith("#bigdata").toString(),1))
  
  hashPairs.print()*/
  
  //subbuTags.saveAsTextFiles("/home/spark/Documents/inpudata/twitter1.txt")
  
  
  ssc.start()
  ssc.awaitTermination()
}