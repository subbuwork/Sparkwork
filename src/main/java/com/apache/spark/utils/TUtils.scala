package com.apache.spark.utils

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds

trait TUtils extends App{
  val conf = new SparkConf(true)
                 .setAppName(getClass.getSimpleName)
                 .setMaster("local[*]")
  // Connect to the Spark cluster:
  lazy val sc = new SparkContext(conf)
  sc.setLogLevel("WARN")
  val ssc = new StreamingContext(sc,Seconds(10))
}
object TUtils{
  def apply():TUtils = new TUtils{}
}