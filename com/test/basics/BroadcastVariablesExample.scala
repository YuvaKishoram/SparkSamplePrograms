package com.test.basics

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object BroadcastVariablesExample {
  
   def main(args: Array[String]): Unit = {
     
    val conf = new SparkConf().setAppName(getClass.getName).setMaster("local")
    
    val sc = new SparkContext(conf)
  	
    val data = sc.parallelize(Seq(1,2,3))
    //val broadcastData = sc.broadcast(data.collect)
    println("Printing ActualData... " + data.collect().deep)
    
    var scalaVar = 1
    var broadcastScalaData = sc.broadcast(scalaVar)
    println("Printing broadcastData... " + broadcastScalaData.value)
    
    /*
     * set of operations in executors by using broadcastScalaData
     */
    println("Printing firstBroadCastResult... ")
    val firstBroadCastResult = data.map(number => number*broadcastScalaData.value)
    firstBroadCastResult.foreach(println)
    
    scalaVar= 2
    broadcastScalaData = sc.broadcast(scalaVar)
    println("Printing modifed broadcastData... " + broadcastScalaData.value)
    
    /*
     * set of operations in executors by using broadcastScalaData
     */
    println("Printing secondBroadCastResult... ")
    val secondBroadCastResult = data.map(number => number*broadcastScalaData.value)
    secondBroadCastResult.foreach(println)
    /*
     * java.lang.IllegalArgumentException: requirement failed: Can not directly broadcast RDDs; 
     * instead, call collect() and broadcast the result.
  	 */
    
    /*
     * Broadcast a read-only variable to the cluster, returning a org.apache.spark.broadcast.Broadcast object for reading it in distributed functions. 
     * The variable will be sent to each cluster only once.
     */ 
    
    //println("Printing broadcastData... " + broadcastData.value.deep) //Get the broadcasted value.
    
    //broadcastData.destroy()
    /*Destroy all data and metadata related to this broadcast variable. Use this with caution; 
    once a broadcast variable has been destroyed, it cannot be used again. 
    This method blocks until destroy has completed*/ 

    //println("Printing broadcastData... " + broadcastData.value.deep) /* org.apache.spark.SparkException: Attempted to use Broadcast(1) after it was destroyed*/
   
  println("completed")
  }
}