package com.apache.spark.sql.cassandra

import org.apache.spark.SparkContext._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector._
import com.apache.spark.utils.UtilsApp

object WordCountWithCassandra extends UtilsApp{
  
  CassandraConnector(conf).withSessionDo {session=>
    //Creating keyspace 
    session.execute(s"CREATE KEYSPACE IF NOT EXISTS demo WITH REPLICATION={'class':'SimpleStrategy','replication_factor':1}")
    session.execute(s"CREATE TABLE IF NOT EXISTS demo.wordcount(word TEXT PRIMARY KEY, count COUNTER)")
    session.execute(s"TRUNCATE demo.wordcount")
  }
  
  /*we load text file from the local path ,split the 
  file and count the number of occurrences of each word in the file and store same into cassandra table called wordcount*/
  
  sc.textFile(words).flatMap(_.split(" ")).map(word=>(word,1)).reduceByKey((a,b)=>a+b).saveToCassandra("demo", "wordcount")
  
  //Reading from cassandra table
  
  sc.cassandraTable("demo","wordcount").collect().foreach(println)
  
  
  println("Job completed!!!!")
  sc.stop()
  
  
}