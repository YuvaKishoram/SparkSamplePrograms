package com.test.cassandra

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import com.datastax.spark.connector._
import scala.collection.Seq
import com.datastax.driver.core.ConsistencyLevel
import com.datastax.spark.connector.rdd.ReadConf
import com.datastax.spark.connector.{ColumnRef,ColumnName}
    
object CassandraCRUDOperationsUsingRDD {

  def main(args: Array[String]): Unit = {
    
    val sparkConf = new SparkConf().setAppName(getClass.getName)
    sparkConf.setIfMissing("spark.master", "local[*]")
    sparkConf.setIfMissing("spark.cassandra.connection.host", "localhost")
    sparkConf.setIfMissing("spark.cassandra.output.consistency.level", ConsistencyLevel.LOCAL_ONE.toString())
    
	  val sc = new SparkContext(sparkConf)

    /*val data = sc.cassandraTable("key_space_name", "person_test")
    //Delete data from Cassandra table
    data.deleteFromCassandra("key_space_name", "person_test")*/
    
    val deleteData = sc.cassandraTable("key_space_name", "person_test")
                        .where("emp_name='dog'")
                        .deleteFromCassandra("key_space_name", "person_test")
   
    val data = sc.cassandraTable("key_space_name", "person_test")
   data.collect().foreach(println)
  }
}