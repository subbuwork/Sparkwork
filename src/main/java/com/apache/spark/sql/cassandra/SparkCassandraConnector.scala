package com.apache.spark.sql.cassandra

import org.apache.spark.SparkConf
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.cassandra._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext


object SparkCassandraConnector {
  
  def main(args: Array[String]): Unit = {
    
    val SparkMasterHost = "127.0.0.1"

  val CassandraHost = "127.0.0.1"
    
    
    val conf = new SparkConf().setAppName("SparkCassandraConnector")
                              .setMaster("local")
                              .set("spark.cassandra.connection.host", CassandraHost)
    val sc = new SparkContext(conf)
   // val sql = new SQLContext(sc)
    CassandraConnector(conf).withSessionDo { session =>
      session.execute("INSERT INTO test.emp(emp_id,emp_city, emp_name, emp_phone,emp_sal) VALUES (5,'chennai','manager',123456789,260000)")
      session.execute("INSERT INTO test.emp(emp_id,emp_city, emp_name, emp_phone,emp_sal) VALUES (6,'chennai','Developer',1783456789,280000)")
      session.execute("INSERT INTO test.emp(emp_id,emp_city, emp_name, emp_phone,emp_sal) VALUES (7,'hyderbad','manager',123906789,290000)")
      session.execute("INSERT INTO test.emp(emp_id,emp_city, emp_name, emp_phone,emp_sal) VALUES (8,'mumbai','analyst',1234789789,230000)")
      session.execute("INSERT INTO test.emp(emp_id,emp_city, emp_name, emp_phone,emp_sal) VALUES (9,'chennai','developer',1234890789,230000)")
      session.execute("INSERT INTO test.emp(emp_id,emp_city, emp_name, emp_phone,emp_sal) VALUES (10,'pune','manager',1234336789,25670000)")
      
    }
                
    sc.stop()
    
    
  }
  
}