package com.test.basics

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object UserDefinedFilterExample {
  
  def wordsWithC(data : String): String  = {
        println("Inside wordsWithC method received data : "+ data)
        
        if(data.toLowerCase().startsWith("c"))
          data
        else
         Nil.toString
    }
  
  def filterWords(fileData : RDD[String]) = {
    /* wrong way of doing - it will give empty data in result for else case
     
      val res = for(f <- fileData ) yield { if(f.toLowerCase().startsWith("c")) f} 
       println("Printing res... " + res.collect().deep)
     */

     /* Another way of doing for loop and yield
      	val res2 = for(f <- fileData if(f.toLowerCase().startsWith("c"))) yield f
      	println("Printing res2... " + res2.collect().deep)
    */
        
    val res1 = for{
      f <- fileData // warning : value withFilter is not a member of org.apache.spark.rdd.RDD[String]
       if(f.toLowerCase().startsWith("c"))
    }yield f
    
    println("Printing res1... " + res1.collect.deep)
    
    res1
  }
  
  def main(args: Array[String]): Unit = {
  
    val conf = new SparkConf().setAppName(getClass.getName).setMaster("local")
    
    val sc = new SparkContext(conf)
  	
  	val fileData = sc.parallelize(Seq("Apple","Bannana","Cat","Rat","Mat","Boat","Car"))
  	println("Printing fileData... "+ 	fileData.collect.deep)
  	
  	// will give empty data if condition is false
    val wordsWithCRDD = fileData.map(wordsWithC)
    println("Printing wordsWithCRDD... " + wordsWithCRDD.collect().deep)
    
	val res = filterWords(fileData)
  	println("Printing res... " + res.collect().deep)
 	
  }
}