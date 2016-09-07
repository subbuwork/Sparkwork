package com.apache.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object AccumulatorTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().
               setAppName("AccumulatorTest").
               setMaster("local")
               
    val sc   = new SparkContext(conf)
    
    //Creating accumulator
    var accu = sc.accumulator(0)
    println("Accumualtor initial value::"+accu.value)
    
    // creating RDD from local file
    val data = sc.textFile("/home/spark/Documents/Input.txt")
    
    // converting line into words of array and adding length of the each word to the accumulator
    data.flatMap(_.split(",")).foreach(word=>accu.add(word.length()))
    println("Accumulator value after adding words length::::"+accu.value)
    
    //overriding to initial value zero
    accu= sc.accumulator(0)
    println("Initial  value::::"+accu.value)
    
    //stopping spark context
    sc.stop()
    
  }
}