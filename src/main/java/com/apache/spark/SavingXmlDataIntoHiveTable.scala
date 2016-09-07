package com.apache.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.SaveMode

object SavingXmlDataIntoHiveTable {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf(true).setAppName(getClass.getSimpleName).setMaster("local")
    val sc   = new SparkContext(conf)
    val hc   = new HiveContext(sc)
    hc.setConf("hive.metastore.uris","thrift://localhost:9083")
    hc.sql("use hivedb")
    
    val df = hc.read.format("com.databricks.spark.xml")
                    .option("rootTag","root")
                    .option("rowTag","row").load("/home/spark/Documents/inputdata/emp.xml")
    val table = df.withColumn("id",df("id").cast(IntegerType))
    table.write.mode(SaveMode.Append).saveAsTable("xml_emp")
    
    println("Job completed!!!!!!")
    sc.stop()
  }
}