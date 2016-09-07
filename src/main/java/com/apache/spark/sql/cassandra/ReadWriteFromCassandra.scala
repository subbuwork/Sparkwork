package com.apache.spark.sql.cassandra

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.cassandra._

object ReadWriteFromCassandra {
  
  def main(args: Array[String]): Unit = {
    val cassandraHost= "127.0.0.1"
    val conf = new SparkConf().setAppName("ReadWriteFromCassandra")
                              .setMaster("local")
                              .set("spark.cassandra.connection.host",cassandraHost)
    val sc = new SparkContext(conf)
    
    val sqlcontext = new SQLContext(sc)
    
    sqlcontext.read.cassandraFormat("emp","test").load().registerTempTable("emptmp")
    
    val result = sqlcontext.sql("SELECT emp_city,count(*) as count from emptmp group by emp_city")
    result.collect().foreach(println)
    
  
  }                          
  
  
}