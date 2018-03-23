package com.test.basics

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object RDDJoinsUsingCaseClass {
  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf().setMaster("local[*]").setAppName("RDDJoins")
    val sc = new SparkContext(conf)
    
      val empRdd = sc.parallelize(List(
                                    ("Kishore",11,1),("Anil",22,2),
                                    ("ABC",11,3),("XYZ",11,1)//,("None",null,0)
                                    )
                                )
  
      val empMap = empRdd.map(x => (x._3,EmpCaseClass(x._2.toLong,x._1,x._3.toLong)))
      
      val deptRdd = sc.parallelize(List(
                                    ("TCS",1),("CTS",2)
                                    )
                                )
                                
      val deptMap = deptRdd.map(x => (x._2,DeptCaseClass(x._1, x._2.toLong))) 
      
      val join = empMap.join(deptMap)
      join.collect
                                 
      val leftOuterJoin = empMap.leftOuterJoin(deptMap)
      leftOuterJoin.collect()
      
      val rightOuterJoin = empMap.rightOuterJoin(deptMap)
      rightOuterJoin.collect()
      
      val fullOuterJoin = empMap.fullOuterJoin(deptMap)
      fullOuterJoin.collect()
      
  }
}

case class DeptCaseClass(name:String, id:Long)
case class EmpCaseClass(id:Long, name:String, deptId:Long)