package com.apache.spark.sql.cassandra
import com.datastax.spark.connector.cql.CassandraConnector

import com.apache.spark.utils.UtilsApp

object BasicReadWriteOperations extends UtilsApp {
    //Connecting to cassandra and doing operatins with cssandra session
  
    CassandraConnector(conf).withSessionDo {session =>
    session.execute(s"CREATE TABLE IF NOT EXISTS test.key_value(key INT PRIMARY KEY, value VARCHAR)")
    session.execute(s"TRUNCATE test.key_value")
    
    //Inserting values into key_value table
    session.execute("INSERT INTO test.key_value(key, value) VALUES (1, 'first row')")
    session.execute("INSERT INTO test.key_value(key, value) VALUES (2, 'second row')")
    session.execute("INSERT INTO test.key_value(key, value) VALUES (3, 'third row')")
  }
  //// Read table test.kv and print its contents:
  import com.datastax.spark.connector._
  val rdd = sc.cassandraTable("test","key_value")
  rdd.collect().foreach(row=>println(s"Existing row data::$row"))
  
  // Write two new rows to the test.kv table:
  val input = sc.parallelize(Seq((4,"fourth row"),(5,"fifth row")))
  input.saveToCassandra("test","key_value",SomeColumns("key","value"))
  
  assert(input.collect.length==2)
  
  input.collect.foreach(row=>println(s"New Data::$row"))
  
  println("Job completed")
  
}