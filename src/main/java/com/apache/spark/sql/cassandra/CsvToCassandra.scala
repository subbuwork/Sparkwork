package com.apache.spark.sql.cassandra

import com.datastax.spark.connector.SomeColumns
import com.apache.spark.utils.UtilsApp
import com.datastax.spark.connector.cql.CassandraConnector

object CsvToCassandra extends UtilsApp {
  case class baby_names(year:Int,name:String,gender:String)
  val columns = SomeColumns("year","name","gender")
  
  CassandraConnector(conf).withSessionDo { session =>
    
  session.execute(s"CREATE TABLE IF NOT EXISTS test1.babynames(year int primary key,name text,gender text)")
  
  }
  
  var csvFile = sc.textFile("/home/spark/Documents/inputdata/baby_names.csv")
  var babytable = csvFile.map(line=>line.replace("\"","")).
                          map(line=>line.split(",")).
                          map(baby=>baby_names(baby(0).trim().toInt,baby(1),baby(3)))
  
  import com.datastax.spark.connector._                       
  babytable.saveToCassandra("test1","babynames",columns) 
  println("Job completed!!!!")
  sc.stop()
}