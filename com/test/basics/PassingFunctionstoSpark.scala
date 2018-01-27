package com.test.basics

import org.apache.spark.{SparkConf,SparkContext}

object PassingFunctionstoSpark {
  
  def printText(line : String) {
        println("Inside printText method line : "+ line) 
    }
  
  def modifyText(line : String): String  = {
        println("Inside modifyText method received line : "+ line)
        line+(" - added this in modifyText")
    }
  
  def main(args: Array[String]): Unit = {
  
  	val conf = new SparkConf().setAppName(getClass.getName).setMaster("local")
    
    val sc = new SparkContext(conf)
  	
  	val fileData = sc.textFile(args(0))//provide the input path in arg0
  	fileData.foreach(println)
  	
  	/*
  	 * Return a new RDD by applying a function to all elements of this RDD.  
  	 */
  	println(fileData.map(printText).collect().deep)
  	fileData.map(printText).foreach(println)
  	
  	println(fileData.map(modifyText).collect().deep)
  	fileData.map(modifyText).foreach(println)
  }
}