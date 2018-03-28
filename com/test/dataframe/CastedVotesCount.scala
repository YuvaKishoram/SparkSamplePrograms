package com.test.dataframe

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object CastedVotesCount {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    							.setAppName("Receive Payment Kafka")
    							.setMaster("local[*]")
		
	  val sc = new SparkContext(sparkConf)
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._
    
    val votesData = sc.parallelize(Seq(
                                        ("M","Y"),("M","Y"),("M","N"),
                                        ("F","Y"),("F","N"),("F","N")
                                        ))
    val votesDataRdd = votesData.map(d => println(d))
    
    val votesDF = votesData.toDF("Gender","Voted")
    
    votesDF.createOrReplaceTempView("votes")
    
    spark.sql("SELECT Gender,Voted,COUNT(*) FROM votes GROUP BY Gender,Voted ").show
    
    //votesDF.groupBy("Gender").sum("Gender").show()//org.apache.spark.sql.AnalysisException: "Gender" is not a numeric column. Aggregation function can only be applied on a numeric column.
    votesDF.groupBy("Gender").count().show()
    votesDF.groupBy("Gender","Voted").count().show()
  }
}