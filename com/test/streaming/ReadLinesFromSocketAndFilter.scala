package com.test.streaming

import org.apache.spark._
import org.apache.spark.streaming._


object ReadLinesFromSocketAndFilter {
  
  def main(args:Array[String]){
    
    val conf = new SparkConf().setAppName("ReadLinesFromSocketAndFilter").setMaster("local[*]")

    // Create a StreamingContext with a 30-second batch size from a SparkConf    
    val ssc = new StreamingContext(conf, Seconds(30))
    
    // Create a DStream using data received after connecting to port 7777 on the local machine    
    val lines = ssc.socketTextStream("localhost", 7777)

    lines.print

    // Filter our DStream for lines with "error"    
    val errorLines = lines.filter(_.contains("error"))
    
    // Print out the lines with errors    
    errorLines.print()
   
    //Start our streaming context and wait for it to "finish"
    ssc.start()
    
    // Wait for the job to finish
    ssc.awaitTermination()
  
  }
}