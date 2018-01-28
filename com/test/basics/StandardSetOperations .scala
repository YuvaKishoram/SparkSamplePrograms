package com.test.basics

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object StandardSetOperations  {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(getClass.getName).setMaster("local")
    
    val sc = new SparkContext(conf)
  	
    val data1 = sc.parallelize(Seq(1,2,3))
    val data2 = sc.parallelize(Seq(1,3,5,7,9))
    
    println("data1 : "+data1.collect.deep)
    println("data2 : "+data2.collect.deep)
    
    val unionData = data1.union(data2)
    println("unionData : "+unionData.collect.deep)
    
    val intersectionData = data1.intersection(data2)
    println("intersectionData : "+intersectionData.collect.deep)
    
    val distinctData = unionData.distinct
    println("distinctData : "+distinctData.collect.deep)
    
  }
}