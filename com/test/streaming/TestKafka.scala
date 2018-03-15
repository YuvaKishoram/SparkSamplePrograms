package com.test.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka.KafkaUtils
import kafka.serializer.StringDecoder
import org.apache.log4j.Logger

object TestKafka {
val localLogger = Logger.getLogger("TestKafka")
  def main(args: Array[String]): Unit = {
	  val sparkConf = new SparkConf()
    							.setAppName("TestKafka")
    							.setMaster("local[5]")
	sparkConf.setIfMissing("spark.streaming.blockInterval", "15s");
	  
	val kafkaTopicRaw = "test-topic"
    val kafkaBroker = "127.0.01:9092"
//    sparkConf.setIfMissing("spark.checkpoint.dir", checkpointDir)

    val ssc = new StreamingContext(sparkConf, Seconds(30))

    val topics: Set[String] = kafkaTopicRaw.split(",").map(_.trim).toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> kafkaBroker)

    localLogger.info(s"connecting to brokers: $kafkaBroker")
    localLogger.info(s"kafkaParams: $kafkaParams")
    localLogger.info(s"topics: $topics")

    val rawPaymentStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    rawPaymentStream.print

 //Kick off
    ssc.start()

    ssc.awaitTermination()

    //ssc.stop()
  }

}