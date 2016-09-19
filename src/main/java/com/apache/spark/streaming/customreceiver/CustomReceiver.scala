package com.apache.spark.streaming.customreceiver

import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.storage.StorageLevel
import java.net.Socket
import java.io.BufferedReader
import java.io.InputStreamReader
import java.nio.charset.StandardCharsets
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds

class CustomReceiver(hostName:String,port:Int) extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2){
  
  def onStart(){
    new Thread("Receiver Thread!!!!!!!"){
      override def run(){receiver()}
    }.start()
  }
  
  private def receiver(){
    
    var socket: Socket = null
    var userInput: String = null
    
    try {
      
      socket = new Socket(hostName,port)
      
      val reader = new BufferedReader(new InputStreamReader(socket.getInputStream,StandardCharsets.UTF_8))
      userInput = reader.readLine()
      
      while(!isStopped() && userInput!=null){
        store(userInput)
        userInput = reader.readLine()
        
      }
      reader.close()
      socket.close()
      restart("Tring to reconnect!!!!!!")
      
    } catch {
      case e:java.net.ConnectException => restart("Error connecting to "+hostName+":"+port,e)
      case t: Throwable => restart("Error receiving data::",t)
    }
    
    
    
  }
  
  def onStop(){
    
  }
}

