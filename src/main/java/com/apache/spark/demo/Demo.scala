package com.apache.spark.demo

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.Accumulator

object Demo  {
   var len = 0 
    var word1 = ""
  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf().setAppName(getClass.getSimpleName).setMaster("local[*]")
    conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    
    val sc = new SparkContext(conf)
    //var acc = sc.accumulator(0)
    
    val textFile = sc.textFile("/home/spark/Documents/inputdata/input.txt")
    val words = textFile.flatMap(line => line.split(" "))
   
    words.foreach {word=>
      if(word.size>len){
        len = word.size
        word1 = word 
        
      }
    }
   println("Big word=>"+word1+"::len=>"+len)
    sc.stop()
    
  }
}