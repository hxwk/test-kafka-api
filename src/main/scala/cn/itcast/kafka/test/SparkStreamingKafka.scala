package cn.itcast.kafka.test

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/*
  * @Author: itcast
  * @Date: 2020/3/4 10:12
  * @Description: 主要实现读取kafka数据，并手动提交offset
  * 使用的spark-streaming-kafka-0-10_2.11的direct模式整合kafka
  */
object SparkStreamingKafka {
  def main(args: Array[String]): Unit = {
    //1.创建streamingcontext
    val conf: SparkConf = new SparkConf().setAppName("wc").setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val ssc = new StreamingContext(sc, Seconds(5))
    ssc.checkpoint("./ssc")
    //2.连接kafka的参数设置
    val kafkaParams: Map[String, Object] = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "node01:9092,node02:9092,node03:9092",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.GROUP_ID_CONFIG -> "ssc",
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (false: java.lang.Boolean)
    )
    //3.设置连接的主题参数
    val topics = Array("test")
    //如果是手动提交的offset，需要再这边先获取到offset
    /*val offsets = OffsetUtil.getOffsetMap("ssc","test")
    if(offsets>0){//说明mysql存储了偏移量，那么从当前位置开始消费

    }else{//如果没有便宜量，从最新latest开始消费

    }*/

    //4.连接对应的分区，接收数据
    val recordDstream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
      //手动从mysql获取偏移量
      //ConsumerStrategies.Subscribe[String, String](topics, kafkaParams,offsets)
    )
    //5.接下来手动维护提交偏移量offset，什么时候提交？
    //应该是消费了一小批数据就该提交，而在Dstream中一小批数据就RDD，即消费了RDD中的数据就该提交了
    //我们显示的对RDD进行转换可以使用Transform（转换）和foreach（动作）
    recordDstream.foreachRDD(rdd => {
      if (rdd.count() > 0) { //如果有数据再进行消费并提交
        var offsetRanges: Array[OffsetRange]= null
        rdd.foreach(f => print("接收到的record的消息: " + f))
        //处理完了， 该提交偏移量了，那么spark给我们提供了一个类方便我们来封装偏移量信息
        offsetRanges= rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        for (o <- offsetRanges){
          println(s"topic:${o.topic},partition:${o.partition},fromOffset:${o.fromOffset},untilOffset:${o.untilOffset}")
        }
        //提交offset，提交到checkpoint
        recordDstream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
        //提交到mysql数据库中
        //OffsetUtil.saveOffsetRanges()
      }
    })

    //开启任务
    ssc.start()
    //等到终止
    ssc.awaitTermination()

  }
}
