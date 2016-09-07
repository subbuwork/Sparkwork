package com.apache.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.SaveMode

object SavingDataInotHiveTable {
  
  case class baby(year:Int,name:String,prob:Float,gender:String)
  
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SavingDataInotHiveTable")
                              .setMaster("local")

    val sc   = new SparkContext(conf)
    
    val hc   = new HiveContext(sc)
    hc.setConf("hive.metastore.uris","thrift://localhost:9083")
    hc.sql("use hivedb")
    val source = sc.textFile("/home/spark/Documents/inputdata/baby_names.csv")
    import hc.implicits._
    val table  = source.map(line=>line.replace("\"", ""))
                       .map(line=>line.split(","))
                       .map(b=>baby(b(0).trim().toInt,b(1),b(2).trim().toFloat,b(3))).toDF
    
    table.write.format("orc").mode(SaveMode.Append).saveAsTable("baby_names")
    
    
    println("Job completed!!!")
 
  }
}