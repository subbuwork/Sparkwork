package com.apache.spark.analytics

import com.apache.spark.utils.UtilsApp
import com.datastax.spark.connector.SomeColumns
import com.datastax.spark.connector.cql.CassandraConnector

object Consumer_Complaints extends UtilsApp {
  
  case class consumer_complaints(date:String,Proudct:String,SubProduct:String,Issue:String,SubIssue:String,Company:String,
                                 State:String,SubmittedVia:String,DataSentToCompany:String,Response:String,
                                 TimelyResponse:String,ConsumerDisputed:String,ComplaintId:Int)
                                 
  val columns = SomeColumns("date","Product","SubProduct","Issue","SubIssue","Company","State","SubmittedVia","DateSentToCompany"
                           ,"Response","TimelyResponse","ConsumerDisputed","ComplaintId") 
  CassandraConnector(conf).withSessionDo {session =>
    session.execute(s"CREATE TABLE consumer_complaints(date text,Product text,SubProduct text,Issue text,SubIssue text, Company text,"+
                     "State text, SubmittedVia text,DateSentToCompany text,Response text,TimelyResponse text,"+
                     "ConsumerDisputed text,ComplaintId int primary key"+")")
  }                         
  
  
  
  
  
  
  
  
}