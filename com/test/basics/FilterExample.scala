package com.test.basics

import org.apache.spark.{SparkConf, SparkContext}

object FilterExample {
  
  def main(args: Array[String]): Unit = {
  
    val conf = new SparkConf().setAppName(getClass.getName).setMaster("local")
    
    val sc = new SparkContext(conf)
  	
  	val fileData = sc.parallelize(Seq(1,2,3,4,5,6,7,8,9,10))
  	println("Printing fileData... "+ 	fileData.collect.deep)
  	
    //val wrongWayFilter1 = fileData.filter(x>5 && x<8) //error: value > is not a member of org.apache.spark.rdd.RDD[String]
  	
  	//val wrongWayFilter2 = fileData.filter(_>5 && _<8)
  	/*error: missing parameter type for expanded function ((x$1, x$2) => x$1.$greater(5).$amp$amp(x$2.$less(8)))
       val wrongWayFilter2 = fileData.filter(_>5 && _<8)
                                             ^
			error: missing parameter type for expanded function ((x$1: <error>, x$2) => x$1.$greater(5).$amp$amp(x$2.$less(8)))
       val wrongWayFilter2 = fileData.filter(_>5 && _<8) */
  	
  	///val wrongWayFilter3 = fileData.filter(_ => (_>5 && _<8))
  	
  	//Filter Return a new RDD containing only the elements that satisfy a predicate.
  	
  	val filterByAgeData = fileData.filter(x => (x>5 && x<8))
    
  	println("Printing filteredData... " + filterByAgeData.collect().deep)

  	val anothorWayFilter = fileData.filter(_1 => (_1>5 && _1<8))
  	println("Printing anothorWayFilter... " + anothorWayFilter.collect().deep)
  	
  	
  }
}