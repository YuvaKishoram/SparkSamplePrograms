package com.test.dataframe

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DataFrameJoinsUsingDSL {
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

  /* Spark allows using following join types:  
  
    case "inner" => Inner
    case "outer" | "full" | "fullouter" => FullOuter
    case "leftouter" | "left" => LeftOuter
    case "rightouter" | "right" => RightOuter
    case "leftsemi" => LeftSemi
    case "leftanti" => LeftAnti
    case "cross" => Cross
    case _ =>
      val supported = Seq(
                          "inner",
                          "outer", "full", "fullouter",
                          "leftouter", "left",
                          "rightouter", "right",
                          "leftsemi",
                          "leftanti",
                          "cross"
                          )
   */
/*
  *********** equi joins ********** 
*/
  //Self-join
  val selfJoin = employees.join(employees,"deptID")
  selfJoin.show
  
  val selfJoinUsingExpr = employees.join(employees,employees.col("DeptID")===employees.col("DeptID"))// If we use expression we will get duplicated “DepartmentID” column from both dataframes
  selfJoinUsingExpr.show
  
  //Inner Join (Default Join Type)

 //Using DataFrame
  val innerJoinUsingString = employees.join(departments,"deptID")
  innerJoinUsingString.show //Spark automatically removes duplicated “DepartmentID” column, so column names are unique and one does not need to use table prefix to address them.
  
  val innerJoinUsingSeq = employees.join(departments,Seq("deptID"))
  innerJoinUsingSeq.show
  
  val innerJoinUsingJoinType = employees.join(departments,Seq("deptID"),"inner")
  innerJoinUsingJoinType.show
  
  /* employees.join(departments,employees.column('deptID))//value column is not a member of org.apache.spark.sql.DataFrame
   * employees.join(departments,employees.col("deptID")) //here we need expression
   * org.apache.spark.sql.AnalysisException: join condition '`deptID`' of type int is not a boolean.;;
  */
  
  val naiveInnerJoinUsingExprs = employees.join(departments,employees("DeptID")===departments("DeptID"))
  naiveInnerJoinUsingExprs.show
  
  naiveInnerJoinUsingExprs.printSchema
/*
root
 |-- Name: string (nullable = true)
 |-- DeptID: integer (nullable = true)
 |-- DeptID: integer (nullable = false)
 |-- DeptName: string (nullable = true)
*/ 
  naiveInnerJoinUsingExprs.select("DeptID")
  //org.apache.spark.sql.AnalysisException: Reference 'DeptID' is ambiguous, could be: DeptID#7, DeptID#21.;

  val explicitInnerJoinUsingExprs = employees.as("e1").join(departments.as("d1"),$"e1.DeptID" === $"d1.DeptID")
  explicitInnerJoinUsingExprs.show
  explicitInnerJoinUsingExprs.select($"e1.DeptID")
  
  val innerJoinUsingExprsWithCol = employees.join(departments,employees.col("DeptID")===departments.col("DeptID"))
  innerJoinUsingExprsWithCol.show //If we use expression we will get duplicated “DepartmentID” column from both dataframes
  
  val selectRequiredColumns = innerJoinUsingExprsWithCol.select(employees.col("Name"),employees.col("DeptID"),departments.col("DeptName"))
  selectRequiredColumns.show
  
  //employees.join(departments,employees.col('DeptID)===departments.col('DeptID))//type mismatch; found : Symbol required: String
  
  val innerJoinUsingExprsJoinType = employees.join(departments,employees.col("DeptID")===departments.col("DeptID"),"inner")
  innerJoinUsingExprsJoinType.show //Here we will get duplicated “DepartmentID” column from both dataframes
  
/*  wrong way of joining we dont have column names like employees.DeptID , departments.DeptID; input columns: [Name, DeptID, DeptID, DeptName]
 *  employees.join(departments,col("employees.DeptID") === col("departments.DeptID"))
 * 	org.apache.spark.sql.AnalysisException: cannot resolve '`employees.DeptID`' given input columns: [Name, DeptID, DeptID, DeptName];
		'Join Inner, ('employees.DeptID = 'departments.DeptID)
*/
  
  //Using Alias
  val empAlias = employees.as("emp")
  val deptAlias = departments.as("dept")
  
  val innerJoinUsingAliasExprsWithCol = empAlias.join(deptAlias,column("emp.DeptID") === col("dept.DeptID"))
  innerJoinUsingAliasExprsWithCol.show
  
  val innerJoinUsingAliasExprsWithoutCol = empAlias.join(deptAlias,$"emp.DeptID" === $"dept.DeptID","inner")
  innerJoinUsingAliasExprsWithoutCol.show
  
  /**
   * Join with another `DataFrame`.
   *
   * Behaves as an INNER JOIN and requires a subsequent join predicate.
   * 
   */
  /* Need to check
   * val innerJoin = employees.join(departments)
   *  innerJoin.show 
  */
  
  /*
   * org.apache.spark.sql.AnalysisException: Detected cartesian product for INNER join between logical plans
	 * Join condition is missing or trivial.
	 * Use the CROSS JOIN syntax to allow cartesian products between these relations.;
   */
  
 
  //Left Outer Join (Or) left join
  
  val leftOuterJoin = employees.join(departments,Seq("deptID"),"left_outer")//Note, that column name should be wrapped into scala Seq if join type is specified.
  leftOuterJoin.show
  
  val leftJoin = employees.join(departments,Seq("deptID"),"left")
  leftJoin.show
  
  val leftOuterJoinUsingExpr = employees.join(departments,employees("DeptID")===departments("DeptID"),"left_outer")
  leftOuterJoinUsingExpr.show
  
  //Right Outer Join (Or) Right join
  
  val rightOuterJoin = employees.join(departments,Seq("deptID"),"right_outer")
  rightOuterJoin.show
  
  val rightJoin = employees.join(departments,Seq("deptID"),"right")
  rightJoin.show
  
  // outer (Or) full (Or) fullouter
  
  val outerJoin = employees.join(departments,Seq("deptID"),"outer")
  outerJoin.show
  
  val fullJoin = employees.join(departments,Seq("deptID"),"full")
  fullJoin.show
  
  val fullouterJoin = employees.join(departments,Seq("deptID"),"fullouter")
  fullouterJoin.show
  
  //leftsemi join
  val leftsemiJoin = employees.join(departments,Seq("deptID"),"leftsemi") // It will fetch matched records from left dataframe with left dataframe columns only
  leftsemiJoin.show

  //leftanti join  
  val leftantiJoin = employees.join(departments,Seq("deptID"),"leftanti")// It will fetch un-matched records from left dataframe with left dataframe columns only
  leftantiJoin.show
  
  //rightsemi join
  val rightSemiJoin = departments.join(employees,Seq("deptID"),"leftsemi") // There is no rightsemi join; just swap the dataframes
  rightSemiJoin.show
  
  //rigtanti join  
  val rightAntiJoin = departments.join(employees,Seq("deptID"),"leftanti")// There is no rightanti join; just swap the dataframes
  rightAntiJoin.show
  
  //Cartesian Join (Or) cross join
  val crossJoinUsingAlias = empAlias.join(deptAlias,column("emp.DeptID") === col("emp.DeptID"),"cross")
  crossJoinUsingAlias.show
  
  val crossInnerJoin = employees.join(departments,employees.col("DeptID")===departments.col("DeptID"),"cross")//working like inner join
  crossInnerJoin.show
  
  //val cartesianJoin = employees.join(departments,Seq("deptID"),"cross")//java.lang.IllegalArgumentException: requirement failed: Unsupported using join type Cross
  //cartesianJoin.show
  //Warning: do not use cartesian join with big tables in production.
  //employees.join(departments,employees.col("DeptID")===departments.col("DeptID"),"cross")
   
  
  
  /*
   * All the above explained joins are equi-joins
   */
       
  /*
  	*********** non-equi joins ********** 
	*/
  
  val innerJoinUsingCondition = employees.join(departments, employees.col("DeptID")===departments.col("DeptID") && employees.col("DeptID")>=34)
  innerJoinUsingCondition.show
  
  /*
   * employees.join(departments, employees.col("DeptID")===departments.col("DeptID") && employees.col("DeptID")==34).show
   * It is not giving any records
  */
  
  
/*  Join expression, slowly changing dimensions and non-equi join
		Spark allows us to specify join expression instead of a sequence of columns. 
		In general, expression specification is less readable, so why do we need such flexibility? The reason is non-equi join.

		One application of it is slowly changing dimensions. Assume there is a table with product prices over time:
*/
  val products = spark.sparkContext.parallelize(Array(
                                                      ("steak", "1990-01-01", "2000-01-01", 150),
                                                      ("steak", "2000-01-02", "2020-01-01", 180),
                                                      ("fish", "1990-01-01", "2020-01-01", 100)
                                                    )).toDF("name", "startDate", "endDate", "price")

  products.show()//There are two products only: steak and fish, price of steak has been changed once. 
  
  //Another table consists of product orders by day:
  val orders = spark.sparkContext.parallelize(Array(
                                                    ("1995-01-01", "steak"),
                                                    ("2000-01-01", "fish"),
                                                    ("2005-01-01", "steak")
                                                  )).toDF("date", "product")

  orders.show()
  
  //Our goal is to assign an actual price for every record in the orders table. It is not obvious to do using only equality operators
  val nonEquiJoin = orders.join(products,$"product" === $"name" && $"date">=$"startDate" && $"date"<=$"endDate")
  nonEquiJoin.show
  }
}