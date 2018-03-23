package com.test.dataframe

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DataFrameJoins {
  def main(args: Array[String]): Unit = {
      val spark = SparkSession.builder()
                            .master("local[*]")
                            .appName(getClass.getName)
                            .getOrCreate()
    
    import spark.implicits._
    
    //Employees table has a nullable column. To express it in terms of statically typed Scala, one needs to use Option type.
    val employees = spark.sparkContext.parallelize(Array[(String,Option[Int])]
                                                    (
                                                      ("Kishore",Some(33)),("Anil",Some(33)),
                                                      ("Raghav",Some(34)),("Sunil",Some(34)),
                                                      ("ABC",Some(1)),("XYZ",null)
                                                    )
                                                  ).toDF("Name","DeptID")
    employees.show

    //Department table does not have nullable columns, type specification could be omitted.
    val departments = spark.sparkContext.parallelize(Array(
                                                  (34,"IT"),(33,"CTS"),
                                                  (123,"Other"))
                                                  ).toDF("DeptID","DeptName")
  departments.show
  
  //Inner Join

  //Using SQL
  employees.createOrReplaceTempView("emp")
  departments.createOrReplaceTempView("dept")
  
  val join = spark.sql("SELECT * FROM emp JOIN dept ON emp.DeptID=dept.DeptID")
  join.show()
  
  val innerJoin = spark.sql("SELECT * FROM emp INNER JOIN dept ON emp.DeptID=dept.DeptID")
  innerJoin.show()
 
  val leftJoin = spark.sql("SELECT * FROM emp LEFT JOIN dept ON emp.DeptID=dept.DeptID")
  leftJoin.show()
  
  val rightJoin = spark.sql("SELECT * FROM emp RIGHT JOIN dept ON emp.DeptID=dept.DeptID")
  rightJoin.show()
  
  val fullJoin = spark.sql("SELECT * FROM emp FULL JOIN dept ON emp.DeptID=dept.DeptID")
  fullJoin.show()
  
  val outerJoin = spark.sql("SELECT * FROM emp OUTER JOIN dept ON emp.DeptID=dept.DeptID")
  outerJoin.show()
  
  val crossJoin = spark.sql("SELECT * FROM emp CROSS JOIN dept ON emp.DeptID=dept.DeptID")
  crossJoin.show()
  
  }
}