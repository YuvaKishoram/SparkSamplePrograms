package com.test.dataframe

import org.apache.spark.sql.SparkSession

object DataFrameSQLOperations {
  
  def main(args:Array[String]){
    
    val spark = SparkSession.builder()
                            .master("local[*]")
                            .appName(getClass.getName)
                            .getOrCreate()
                            
    val peopleDF = spark.read.json("People.json")
    
    peopleDF.printSchema()// Print the schema in a tree format
    
    peopleDF.createOrReplaceTempView("people")// Register the DataFrame as a SQL temporary view
    
    spark.sql("select * from people").show()//Select ALL
    spark.sql("select name,age from people").show()//Change the column order
    spark.sql("select name from people").show()//Select only the "name" column
    spark.sql("select name as EmpName from people").show()//Select only the "name" column and rename it
    
    spark.sql("select name,age,age+1 from people").show()//increment the age by 1
    spark.sql("select name,age,age+1 as NewAge from people").show()//increment the age by 1 and rename the new column
    
    spark.sql("select * from people where age>32").show()// age above 32
    spark.sql("select * from people where age>30 and age<34").show()//age between 31 and 33
    spark.sql("select name,age from people where age>33 or age<5 order by age").show()//age above 33 or below 5 sort by age
    
    spark.sql("select age,count(*) from people where group by age").show()//group people by age and count
    
    spark.sql("select max(age) as MaxAge,min(age) as MinimumAge from people").show()//MaximumAge and MinimumAge
    
    spark.sql("select max(age),min(age),avg(age),sum(age),count(age) from people").show()//aggregate functions
    
    // Register the DataFrame as a global temporary view
    peopleDF.createGlobalTempView("people")

    // Global temporary view is tied to a system preserved database `global_temp`
    spark.sql("SELECT * FROM global_temp.people").show()
    
    // Global temporary view is cross-session
    spark.newSession().sql("SELECT * FROM global_temp.people").show()

  }
}