package com.apache.spark.twitter

import com.apache.spark.utils.TUtils
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.Seconds

object SentimentAnalysis extends TUtils {
  
  //Setting twitter personal access and consumer key details
  
  System.setProperty("twitter4j.oauth.consumerKey", "Add your details!!!!")
  System.setProperty("twitter4j.oauth.consumerSecret", "")
  System.setProperty("twitter4j.oauth.accessToken", "")
  System.setProperty("twitter4j.oauth.accessTokenSecret", "")  
 
  
  // Use the streaming context and the TwitterUtils to create the
  // Twitter stream.
  val tStreaming = TwitterUtils.createStream(ssc,None)
  
  
  // Each tweet comes as a twitter4j.Status object, which we can use to
  // extract hash tags. We use flatMap() since each status could have
  // ZERO OR MORE hashtags.
  val hashTags = tStreaming.flatMap(status=>status.getHashtagEntities)
  
  // Convert hashtag to (hashtag, 1) pair for future reduction.
  val hashTagPairs = hashTags.map(tag=>("#"+tag.getText,1))  
  
  // Use reduceByKeyAndWindow to reduce our hashtag pairs by summing their
  // counts over the last 10 seconds of batch intervals (in this case, 2 RDDs).
  
  val topCounts10 = hashTagPairs.reduceByKeyAndWindow((l,r)=>{l+r},Seconds(20))
  
  
  // topCounts10 will provide a new RDD for every window. Calling transform()
  // on each of these RDDs gives us a per-window transformation. We use
 // this transformation to sort each RDD by the hashtag counts. The FALSE
 // flag tells the sortBy() function to sort in descending order.
  
  val sortedTopCounts10 = topCounts10.transform(rdd=>rdd.sortBy(hashTagpair=>hashTagpair._2,false))
  
  // Print popular hashtags.
  
  sortedTopCounts10.foreachRDD(rdd=>{
    val topList = rdd.take(10)
    println("\nPopular topics in last 10 seconds (%s total):".format(rdd.count()))
    topList.foreach{case (tag, count) => println("%s (%d tweets)".format(tag, count))}

  })
  
  
 // Finally, start the streaming operation and continue until killed.
  ssc.start()
  ssc.awaitTermination()
}