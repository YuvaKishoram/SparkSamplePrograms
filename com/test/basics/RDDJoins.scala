package com.test.basics

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object RDDJoins {
  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf().setMaster("local[*]").setAppName("RDDJoins")
    val sc = new SparkContext(conf)
    
    val empRdd = sc.parallelize(List(
                                    ("11","Kishore"),("22","Anil"),
                                    ("100","ABC"),("11","XYZ"),
                                    ("123",null)
                                    )
                                )
  
      val deptRdd = sc.parallelize(List(
                                          ("11","TCS"),("22","CTS")
                                        )
                                  )
                                
      val join = empRdd.join(deptRdd)
      join.collect
                                 
      val leftOuterJoin = empRdd.leftOuterJoin(deptRdd)
      leftOuterJoin.collect()
      
      val rightOuterJoin = empRdd.rightOuterJoin(deptRdd)
      rightOuterJoin.collect()
      
      val fullOuterJoin = empRdd.fullOuterJoin(deptRdd)
      fullOuterJoin.collect()
  }
}