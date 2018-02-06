package com.test.basics

import org.apache.spark.sql.SparkSession

object DataFrameSQLOperations {
  
  def main(args:Array[String]){
    
    val spark = SparkSession.builder()
                            .master("local[*]")
                            .appName(getClass.getName)
                            //.config("spark.some.config.option", "some-value")
                            .getOrCreate()
//The dialect that is used for SQL parsing can be configured with 'spark.sql.dialect'
                            
    val jsonDF = spark.read.json("People.json")
    
    jsonDF.printSchema()// Print the schema in a tree format
    
    jsonDF.createOrReplaceTempView("people")// Register the DataFrame as a SQL temporary view
    
    val sqlDF = spark.sql("select * from people")
    sqlDF.show()
    
    // Register the DataFrame as a global temporary view
    jsonDF.createGlobalTempView("people")

    // Global temporary view is tied to a system preserved database `global_temp`
    spark.sql("SELECT * FROM global_temp.people").show()
    
    // Global temporary view is cross-session
    spark.newSession().sql("SELECT * FROM global_temp.people").show()

  }
}