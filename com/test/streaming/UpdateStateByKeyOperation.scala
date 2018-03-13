package com.test.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds

object UpdateStateByKeyOperation {
  
  def main(args:Array[String]){
    
      val conf = new SparkConf().setAppName("UpdateStateByKeyOperation").setMaster("local[*]")
      val ssc = new StreamingContext(conf,Seconds(30))
      
      ssc.checkpoint("CheckPoint_Dir")//Note that using updateStateByKey requires the checkpoint directory to be configured
      
      val inputData = ssc.socketTextStream("localhost", 7777)
      inputData.print()
      
      val words = inputData.flatMap(_.split(" "))
      val pairs = words.map((_,1))
      
      val wordsCount = pairs.reduceByKey(_+_)
      wordsCount.print()
      
      val updateStateCount = pairs.updateStateByKey[Int](updateCount _)
      updateStateCount.print()
      
      ssc.start()
      ssc.awaitTermination()
  }
  
  def updateCount(newValues : Seq[Int], runningCount : Option[Int]) : Option[Int] = {
    //println("newValues : "+newValues + "\t runningCount : "+ runningCount)
    println(s"newValues : $newValues \t runningCount : $runningCount")
    
    val currentCount = newValues.sum
    val prevCount =  runningCount.getOrElse(0)
    
    Some(currentCount+prevCount)// add the new values with the previous running count to get the new count
  }
}