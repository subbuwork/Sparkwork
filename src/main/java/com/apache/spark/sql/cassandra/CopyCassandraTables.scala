package com.apache.spark.sql.cassandra

import com.datastax.spark.connector.cql.CassandraConnector
import com.apache.spark.utils.UtilsApp

object CopyCassandraTables extends UtilsApp  {
    
 
  CassandraConnector(conf).withSessionDo { session =>
  //Creating tables   
  session.execute(s"CREATE TABLE IF NOT EXISTS test1.source (key INT PRIMARY KEY, data VARCHAR)")  
  session.execute(s"CREATE TABLE IF NOT EXISTS test1.destination (key INT PRIMARY KEY, data VARCHAR)")
  session.execute(s"TRUNCATE test.source")
  session.execute(s"TRUNCATE test.destination")
  
  //Inserting values into source table
  session.execute(s"INSERT INTO test1.source(key, data) VALUES (1, 'first row')")
  session.execute(s"INSERT INTO test1.source(key, data) VALUES (2, 'second row')")
  session.execute(s"INSERT INTO test1.source(key, data) VALUES (3, 'third row')")
  
  import com.datastax.spark.connector._
  
  //Reading data from cassandra table
  val src = sc.cassandraTable("test1","source")
  
  //writing table to cassandra table
  src.saveToCassandra("test1","destination")
  
  val res = sc.cassandraTable("test1", "destination")
  res.collect().foreach(row=>println(s"$row"))
  assert(res.collect().length==3)
  println("Job completed!!!!")
  

  sc.stop()
  
  
  }
}