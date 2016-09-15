import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.Row


object SparkExample {
  case class person(name:String,age:Int)
  def main(args: Array[String]): Unit = {
    val spark = new SparkConf().setAppName(getClass.getSimpleName).setMaster("local[*]")
    
    val sc = new SparkContext(spark)
    val sql = new SQLContext(sc)
    
    runBasicDataFrameExample(sql)
    runInferSchemaExample(sql)
    runProgrammaticSchemaExample(sql)

    sc.stop()
  }
  
  
  private def runBasicDataFrameExample(sql:SQLContext):Unit={
    print("::::::runBasicDataFrameExample::start")
    val df = sql.read.json("/home/spark/Documents/inputdata/employees.json")
    df.show()
    
    import sql.implicits._
    df.printSchema()
    
    df.select("name").show()
    
    df.select(df("age")+1,df("name")).show
    
    df.filter(df("age")>20).show()
    df.groupBy("age").count().show()
  
    df.registerTempTable("people")
    
    sql.sql("select * from people").show()
    
    print("::::::runBasicDataFrameExample::end")
  }
  
  
  private def runInferSchemaExample(sqlContext:SQLContext):Unit={
    print("::::::::runInferSchemaExample:::::start")
    import sqlContext.implicits._
    val people = sqlContext.sparkContext.textFile("/home/spark/Documents/inputdata/people.txt")
    val df = people.map(_.split(",")).map(word=>person(word(0),word(1).trim().toInt)).toDF()
    df.show()
    
    import sqlContext.implicits._
    df.printSchema()
    
    df.select("name").show()
    
    df.select(df("age")+1,df("name")).show
    
    df.filter(df("age")>20).show()
    df.groupBy("age").count().show()
  
    df.registerTempTable("people")
    
    sqlContext.sql("select * from people").show()
    print("::::::::runInferSchemaExample:::::end")
    
  }
  
  private def runProgrammaticSchemaExample(sql:SQLContext):Unit={
    println(":::::::runProgrammaticSchemaExample:: starting")
    
    val schemaString = "name age"
    val schema = StructType(schemaString.split(" ").map(fname=>StructField(fname,StringType,true)))
    
     val peopleRDD = sql.sparkContext.textFile("/home/spark/Documents/inputdata/people.txt")
     val rowRDD = peopleRDD.map(_.split(",")).map(word=>Row(word(0),word(1).trim()))
     val df = sql.createDataFrame(rowRDD,schema)
     
     import sql.implicits._
    df.printSchema()
    
    df.select("name").show()
    
    df.select(df("age")+1,df("name")).show
    
    df.filter(df("age")>20).show()
    df.groupBy("age").count().show()
  
    df.registerTempTable("people")
    
    sql.sql("select * from people").show()
    println("::::::runProgrammaticSchemaExample:: ending")
    
  }
}