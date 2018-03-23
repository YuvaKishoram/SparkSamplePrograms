package com.test.dataframe

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DataFrameDSLOperations {
  
  def main(args:Array[String]){
    
    val spark = SparkSession.builder()
                            .master("local[*]")
                            .appName(getClass.getName)
                            .getOrCreate()
                            
    val peopleDF = spark.read.json("People.json")
    peopleDF.show()
    
    peopleDF.printSchema()// Print the schema in a tree format
    
     // For implicit conversions like converting RDDs to DataFrames
    // This import is needed to use the $-notation
    import spark.implicits._
    
     // 3 different ways to Select only the "name" column
    peopleDF.select("name").show()
    peopleDF.select($"name").show()
    peopleDF.select('name).show()
    
/*    def selectExpr(exprs: String*): DataFrame

			Selects a set of SQL expressions. This is a variant of select that accepts SQL expressions.

      // The following are equivalent:
      ds.selectExpr("colA", "colB as newName", "abs(colC)")
      ds.select(expr("colA"), expr("colB as newName"), expr("abs(colC)"))

			Since	2.0.0
*/
    peopleDF.selectExpr("name","age as empAge","abs(age)").show // use abs and also Rename the column
    
    /* Wrong way of doing
     * peopleDF.selectExpr($"name").show
     * error: type mismatch;     found   : org.apache.spark.sql.ColumnName     required: String
    */
       
    
    peopleDF.select("name","age").show()//Change the column order
    
    /* Wrong way of doing
     * peopleDF.select($"name","age").show()
     * org.apache.spark.sql.DataFrame cannot be applied to (org.apache.spark.sql.ColumnName, String)
    */
    
    // Select everybody, but increment the age by 1
    
    peopleDF.select($"name", $"age" + 1).show()
    peopleDF.select($"name", $"age" + 1 as "New Age").show() // Rename the column
    peopleDF.selectExpr("name","age as org_age","age + 1", s"""age+1 as New_Age """).show()// Rename the column
    
    val filterdByAge = peopleDF.filter($"age">32)//.show    // Select people older than 32
    filterdByAge.map(row => row.toString()).show()
    filterdByAge.map(row => "Hello "+row.getAs[String]("name")).show()

    peopleDF.filter($"age">1 and $"age"<5)    // Select people age between 2 and 4
    peopleDF.filter($"age">30 or $"age"<5).select("name","age").sort("age").show    // Select people age above 30 or below 5 and change the column order then sort by age
    
    peopleDF.groupBy("age").count.show   // //group people by age and count
    //    or
    peopleDF.groupBy("age").agg("age"->"count").show
    
    peopleDF.groupBy().agg("age" -> "max","age" -> "min").show//MaximumAge and MinimumAge
    //    or 
    peopleDF.agg("age" -> "max","age" -> "min").show//MaximumAge and MinimumAge
    //    or
    peopleDF.agg(max("age"),min("age")).show
    
    //The available aggregate methods are `avg`, `max`, `min`, `sum`, `count`
    peopleDF.agg("age" -> "max","age" -> "min","age" -> "avg","age" -> "sum","age" -> "count").show
    
    
  }
}