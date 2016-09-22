package com.apache.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext



object SparkExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
               .setAppName("SparkExample")
    conf.setMaster("local")
               
    val sc   = new SparkContext(conf)
    
    val sql  = new SQLContext(sc)
    
    import sql.implicits._
    
    val df  = sql.read.json("/home/spark/Documents/emp.json")
    //df.show()
    
    df.registerTempTable("emp")
    
    sql.sql("select * from emp").show()
    
    sql.sql("select age ,count(1) count from emp where age>21 group By age order By age").show
    
    sc.stop()
               
               
  }
}