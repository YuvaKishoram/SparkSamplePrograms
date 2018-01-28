package com.test.basics

import org.apache.spark.{SparkConf, SparkContext}

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
  	
  	val fileData = sc.parallelize(Seq("abc","xyz"))//Please provide the input path in arg0
  	println("Printing fileData... "+ 	fileData.collect.deep)
  	
/*  
 *  	println("Printing printText... "+ fileData.map(printText).collect().deep)
 
 *   	println("Printing modifyText... " +fileData.map(modifyText).collect().deep)
 */
	
  	val rddUnit = fileData.map(printText)//map will Return a new RDD by applying a function to all elements of this RDD.
  	println("Printing rddUnit... " + rddUnit.collect().deep)
  	
  	val rddString = fileData.map(modifyText)
  	println("Printing rddString... " + rddString.collect().deep)
  	
  }
}