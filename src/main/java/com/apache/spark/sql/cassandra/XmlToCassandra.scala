package com.apache.spark.sql.cassandra

import com.apache.spark.utils.UtilsApp
import com.datastax.spark.connector.SomeColumns
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.Row

object XmlToCassandra extends UtilsApp{
  
  case class emp(id:Int,name:String,sal:Int)
  val columns = SomeColumns("id","name","sal")
  CassandraConnector(conf).withSessionDo { session =>
   session.execute(s"CREATE TABLE IF NOT EXISTS test1.emp(id int primary key,name varchar,sal int)")  
  }
  
  val sql = new SQLContext(sc)
  
  val df = sql.read.format("com.daatabricks.spark.xml").
                option("rootTag","root").option("rowTab","row")load("/home/spark/Documents/inputdata/emp.xml")
  val table = df.withColumn("id", df("id").cast(IntegerType))  
  
  
}