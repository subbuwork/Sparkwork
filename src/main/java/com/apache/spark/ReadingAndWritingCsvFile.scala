package com.apache.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SaveMode

object ReadingAndWritingCsvFile {
  
  case class emp(id:Int,name:String,prob:Float,state:String)
  
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().
                   setAppName("ReadingAndWritingCsvFile").
                   setMaster("local")
    val sc = new SparkContext(conf)  
    val sql= new SQLContext(sc)
    
    val csvData = sc.textFile("/home/spark/Documents/baby.csv")
    csvData.collect()
    val data = csvData.map(line=>line.replace("\"",""))
    data.collect()
    
    
    val data1 = data.map(line=>line.split(","))
    val data2 = data1.map(e=>emp(e(0).trim().toInt,e(1),e(2).trim().toFloat,e(3)))
    data2.collect()
    
    import sql.implicits._
    data2.toDF.write.format("json").mode(SaveMode.Append).save("hdfs:localhost:9000/user/spark/csv/")
    
    
    
  }
  
  
}