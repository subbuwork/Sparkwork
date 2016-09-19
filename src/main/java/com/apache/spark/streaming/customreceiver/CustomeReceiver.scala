package com.apache.spark.streaming.customreceiver

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import java.net.Socket
import java.io.BufferedReader
import java.io.InputStreamReader
import java.nio.charset.StandardCharsets

object CustomeReceiver {
  
  val conf = new SparkConf().setAppName(getClass().getSimpleName).setMaster("local[*]")
  val ssc  = new StreamingContext(conf,Seconds(5))
  
  val receiverData = ssc.receiverStream(new CustomReceiver2("localhost",9999))
  val words = receiverData.flatMap(line=>line.split(" "))
  val wordsCount = words.map(w=>(w,1)).reduceByKey(_+_)
  wordsCount.print()
  ssc.start()
  ssc.awaitTermination()

}


class CustomReceiver2(host:String,port:Int) extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2){
  
  
  
  
  def onStart(){
    new Thread("Receiver Thread!!!!"){
      override def run(){receiver()}
    }.start()
  }
  
  def onStop(){
    
  }
  
  
 def  receiver(){
   var socket: Socket = null
  var userInput: String = null
  try {
    socket = new Socket(host,port)
    
    val reader = new BufferedReader(new InputStreamReader(socket.getInputStream,StandardCharsets.UTF_8))
    userInput = reader.readLine()
    while(!isStopped() && userInput!=null){
      store(userInput)
      userInput = reader.readLine()
    }
    reader.close()
    socket.close()
    restart("Error connecting "+host+":"+port)
    
  } catch {
    case e : java.net.ConnectException=> restart("Error connecting "+host+":"+port,e)
    case t: Throwable => restart("Error receiving data::",t)
  }
  
}
  
}