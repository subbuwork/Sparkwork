package com.apache.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object ReadingAndWritingXmlFile {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ReadingAndWritingXmlFile").setMaster("local")
    val sc   = new SparkContext(conf)
    val hive = new SQLContext(sc)
    
    val xmlData = hive.read.format("com.databricks.spark.xml").
                           option("rootTag","root").
                           option("rowTag","row").
                           load("/home/spark/Documents/inputdata/emp.xml")
   //xmlData.printSchema()                        
  // xmlData.show()
   
  // xmlData.select("*").distinct().show()
   
   xmlData.select("name","sal").groupBy("sal").count().show()

    
    
    
  }
}