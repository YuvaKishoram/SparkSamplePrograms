package com.test.basics

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object FlatMapExample {
  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf().setAppName(getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    
    val x = sc.parallelize(List("spark rdd example",  "sample example"), 2)
    println(x.collect) //Array[String] = Array(spark rdd example, sample example)

    // map operation will return Array of Arrays in following case
    val y = x.map(x => x.split(" ")) // split(" ") returns an array of words
    println(y.collect) //Array[Array[String]] = Array(Array(spark, rdd, example), Array(sample, example))
  
    // flatMap operation will return Array of words in following case
    val y1 = x.flatMap(x => x.split(" "))
    println(y1.collect) //Array[String] = Array(spark, rdd, example, sample, example)
    
    // rdd y1 can be re-written with shorter syntax in Scala as 
    val y2 = x.flatMap(_.split(" "))
    println(y2.collect) //Array[String] = Array(spark, rdd, example, sample, example)

  }
}