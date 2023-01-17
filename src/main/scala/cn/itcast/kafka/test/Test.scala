package cn.itcast.kafka.test

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object Test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("kv")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(2))

    val dataSet: RDD[(Int, Int)] = sc.parallelize(List((1, 2), (1, 3), (1, 4), (2, 2), (2, 3)))
    val result: RDD[Int] = dataSet.reduceByKey((k, v) => k).map(_._2)
    result.foreach(rdd => println(rdd))
  }
}
