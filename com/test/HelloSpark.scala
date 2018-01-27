package com.test

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object HelloSpark {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
                              .setAppName("HelloSpark")
                              .setMaster("local")/* Sets the Spark master URL to connect to, such as "local" to run locally, 
                              											"local[4]" to run locally with 4 cores, 
                              											or "spark://master:7077" to run on a Spark standalone cluster.
     																						 */
    val sc = new SparkContext(conf)
    
    println("Welcome to Spark with version : "+sc.version)
  }
}