package com.test.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds

object WindowOperations {
  
  def main(args:Array[String]){
    
      val conf = new SparkConf().setAppName("WindowOperations").setMaster("local[*]")
      val ssc = new StreamingContext(conf,Seconds(10))//batch interval - 10 seconds
      
      val inputData = ssc.socketTextStream("localhost", 7777)
      //inputData.print()
      
      val words = inputData.flatMap(_.split(" "))
      val pairs = words.map((_,1))
      
      /*
       * > window length - The duration of the window
       * > sliding interval - The interval at which the window operation is performed
       * These two parameters must be multiples of the batch interval of the source DStream*/

      val wordsCount = pairs.reduceByKeyAndWindow(((a:Int,b:Int)=>a+b), Seconds(30), Seconds(20))// Reduce last 30 seconds of data, every 20 seconds
      wordsCount.print()
      
      ssc.start()
      ssc.awaitTermination()
  }
}