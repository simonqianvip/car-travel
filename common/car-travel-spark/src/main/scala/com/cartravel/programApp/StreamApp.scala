package com.cartravel.programApp

import com.cartravel.hbase.conn.HbaseConnections
import com.cartravel.hbase.hutils.HbaseTools
import com.cartravel.kafka.KafkaManager
import com.cartravel.loggings.Logging
import com.cartravel.spark.{SparkEngine, StreamingAppListener}
import com.cartravel.tools.{JsonParse, StructInterpreter, TimeUtils}
import com.cartravel.utils.GlobalConfigUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.Try

object StreamApp extends Logging {
  //5 192.168.80.200:9092,192.168.80.210:9092,192.168.80.220:9092 test_car test_consumer_car 192.168.80.200:2181,192.168.80.210:2181,192.168.80.220:2181
  def main(args: Array[String]): Unit = {
    //1、从kafka拿数据
    if (args.length < 5) {
      System.err.println("Usage: KafkaDirectStream \n" +
        "<batch-duration-in-seconds> \n" +
        "<kafka-bootstrap-servers> \n" +
        "<kafka-topics> \n" +
        "<kafka-consumer-group-id> \n" +
        "<kafka-zookeeper-quorum> "
      )
      System.exit(1)
    }
    //TODO
    val startTime = TimeUtils.getNowDataMin

    val batchDuration = args(0)
    val bootstrapServers = args(1).toString
    val topicsSet = args(2).toString.split(",").toSet
    val consumerGroupID = args(3)
    val zkQuorum = args(4)
    val sparkConf = SparkEngine.getSparkConf()
    val session = SparkEngine.getSparkSession(sparkConf)
    val sc = session.sparkContext
    val ssc = new StreamingContext(sc, Seconds(batchDuration.toLong))

    val topics = topicsSet.toArray
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> bootstrapServers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> consumerGroupID,
      "auto.offset.reset" -> GlobalConfigUtils.getProp("auto.offset.reset"),
      "enable.auto.commit" -> (false: java.lang.Boolean) //禁用自动提交Offset，否则可能没正常消费完就提交了，造成数据错误
    )

    val manager: KafkaManager = new KafkaManager(zkQuorum, kafkaParams)
    val inputDStream: InputDStream[ConsumerRecord[String, String]] = manager.createDirectStream(ssc, topics)

    inputDStream.print()

    inputDStream.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        offsetRanges.foreach(line => {
          println(s"当前的读取的Topic:${line.topic},partition:${line.partition},fromOffset:${line.fromOffset},untilOffset:{${line.untilOffset}}")
        })

        val doElse = Try {
          val data: RDD[String] = rdd.map(line => line.value())
          data.foreachPartition(partition => {
            //构建连接
            val conn = HbaseConnections.getHbaseConn
            //写业务
            partition.foreach(p => {
              val json = JsonParse.parse(p)
              StructInterpreter.interpreter(json._1,json,conn)

            })
            //注销连接
            conn.close()
          })
        }

        if(doElse.isSuccess){
          //提交偏移量
//          manager.persistOffset(rdd)

          //添加spark Streaming监控
          ssc.addStreamingListener(new StreamingAppListener(session,sparkConf,batchDuration.toInt,"Streamming App",manager,rdd))
        }
      }
    })


    ssc.start()
    ssc.awaitTermination()


  }

}
