package com.test.basics

import org.apache.spark.sql.SparkSession

object DataFrameOperations {
  
  def main(args:Array[String]){
    
    val spark = SparkSession.builder()
                            .master("local[*]")
                            .appName(getClass.getName)
                            //.config("spark.some.config.option", "some-value")
                            .getOrCreate()
//The dialect that is used for SQL parsing can be configured with 'spark.sql.dialect'
                            
    val jsonDF = spark.read.json("People.json")
    //jsonDF.show()
    
    jsonDF.printSchema()// Print the schema in a tree format
    
     // For implicit conversions like converting RDDs to DataFrames
    // This import is needed to use the $-notation
    import spark.implicits._
    jsonDF.select("name").show() // Select only the "name" column
    
/*    def selectExpr(exprs: String*): DataFrame

			Selects a set of SQL expressions. This is a variant of select that accepts SQL expressions.

      // The following are equivalent:
      ds.selectExpr("colA", "colB as newName", "abs(colC)")
      ds.select(expr("colA"), expr("colB as newName"), expr("abs(colC)"))

			Since	2.0.0
*/
    jsonDF.selectExpr("name").show
    
    /*
     * jsonDF.selectExpr($"name").show
     * error: type mismatch;     found   : org.apache.spark.sql.ColumnName     required: String
    */
       
    
    jsonDF.selectExpr("name","age as empAge").show
    
    /*
     * jsonDF.select($"name","age").show()
     * org.apache.spark.sql.DataFrame cannot be applied to (org.apache.spark.sql.ColumnName, String)
    */
    
    jsonDF.select("name","age").show()
    
    jsonDF.select($"name", $"age" + 1).show()// Select everybody, but increment the age by 1
    jsonDF.select($"name", $"age" + 1 as "New Age").show()
    
    val filterdData = jsonDF.filter($"age">32)//.show    // Select people older than 32

    jsonDF.filter($"age">2 and $"age"<5).show    // Select people between 2 and 5

    jsonDF.groupBy("age").count.show   // Count people by age

    //filterdData.map
    
  }
}