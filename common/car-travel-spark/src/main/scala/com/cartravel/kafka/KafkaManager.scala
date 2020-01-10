package com.cartravel.kafka

import java.lang.Long

import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer, NoOffsetForPartitionException}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.zookeeper.data.Stat

import scala.collection.mutable
import scala.reflect.ClassTag
import scala.util.Try
import scala.collection.JavaConversions._

/**
 * Created by angel
 */
class KafkaManager(zkHost: String, kafkaParams: Map[String, Object]) extends Serializable {

  private val (zkClient, zkConnection) = ZkUtils.createZkClientAndConnection(zkHost, 10000, 10000)

  private val zkUtils = new ZkUtils(zkClient, zkConnection, false)

  /**
   * 创建 createDirectStream:InputDStream
   **/
  def createDirectStream[K: ClassTag, V: ClassTag](ssc: StreamingContext, topics: Seq[String]): InputDStream[ConsumerRecord[K, V]] = {

    val groupId = kafkaParams("group.id").toString
    //1)读取Offset
    val topicPartition: Map[TopicPartition, Long] = readOffset(topics, groupId)

    //2)KafkaUtils.createDirectStream --> InputDStream
    val stream: InputDStream[ConsumerRecord[K, V]] = KafkaUtils.createDirectStream[K, V](
      ssc,
      PreferConsistent,
      ConsumerStrategies.Subscribe[K, V](topics, kafkaParams, topicPartition)
    )
    stream
  }

  /**
   * 读取偏移量
   *
   * @param topics
   * @param groupId 消费组
   * @return Map[car-1 , car-2 , Long]
   **/

  private def readOffset(topics: Seq[String], groupId: String): Map[TopicPartition, Long] = {
    //创建空的hashMap
    val topicPartitionMap = collection.mutable.HashMap.empty[TopicPartition, Long]

    //拿topic和partition信息
    val topicAndPartitionMaps: mutable.Map[String, Seq[Int]] = zkUtils.getPartitionsForTopics(topics)

    topicAndPartitionMaps.foreach(topicPartitions => {
      val zkGroupTopicsDirs: ZKGroupTopicDirs = new ZKGroupTopicDirs(groupId, topicPartitions._1)
      topicPartitions._2.foreach(partition => { //迭代分区
        //拼装ZK路径
        val offsetPath = s"${zkGroupTopicsDirs.consumerOffsetDir}/${partition}"

        //返回是否读取成功
        val tryGetTopicPartition = Try {
          //String -->offset
          val offsetTuples: (String, Stat) = zkUtils.readData(offsetPath)
          if (offsetTuples != null) {
            topicPartitionMap.put(new TopicPartition(topicPartitions._1, Integer.valueOf(partition)), offsetTuples._1.toLong)
          }
        }
        //如果失败kafka开始
        if (tryGetTopicPartition.isFailure) {
          val consumer = new KafkaConsumer[String, Object](kafkaParams)
          val topicCollection = List(new TopicPartition(topicPartitions._1, partition))
          //拉取数据
          consumer.assign(topicCollection)
          //消费数据
          val avaliableOffset: Long = consumer.beginningOffsets(topicCollection).values().head

          consumer.close()
          topicPartitionMap.put(new TopicPartition(topicPartitions._1, Integer.valueOf(partition)), avaliableOffset)

        }
      })
    })

    //currentoffset 、 earliestoffset  leatestOffset
    //cur < ear || cur > leaty ==> 矫正-->  ear
    //TODO 矫正
    val earliestOffsets = getEarliestOffsets(kafkaParams, topics)
    val latestOffsets = getLatestOffsets(kafkaParams, topics)
    for ((k, v) <- topicPartitionMap) {
      val current = v
      val earliest = earliestOffsets.get(k).get
      val latest = latestOffsets.get(k).get
      if (current < earliest || current > latest) {
        topicPartitionMap.put(k, earliest)
      }
    }


    topicPartitionMap.toMap
  }

  /**
   * 获取早期的偏移量
   *
   * @param kafkaParams
   * @param topics
   * @return
   */
  private def getEarliestOffsets(kafkaParams: Map[String, Object], topics: Seq[String]) = {
    val newKafkaParams = mutable.Map[String, Object]()
    newKafkaParams ++= kafkaParams
    newKafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    //kafka api
    val consumer = new KafkaConsumer[String, Array[Byte]](newKafkaParams)
    //订阅
    consumer.subscribe(topics)
    val noOffsetForPartitionExceptionSet = mutable.Set()
    try {
      consumer.poll(0)
    } catch {
      case e: NoOffsetForPartitionException =>
      //        noOffsetForPartitionExceptionSet.add(e.partition())
      //邮件报警
    }
    //获取 分区信息
    val topicp = consumer.assignment().toSet
    //暂定消费
    consumer.pause(topicp)
    //从头开始
    consumer.seekToBeginning(topicp)
    val toMap = topicp.map(line => line -> consumer.position(line)).toMap
    val earliestOffsetMap = toMap
    consumer.unsubscribe()
    consumer.close()
    earliestOffsetMap
  }

  /**
   * 获取最新的偏移量
   *
   * @param kafkaParams
   * @param topics
   * @return
   */
  private def getLatestOffsets(kafkaParams: Map[String, Object], topics: Seq[String]) = {
    val newKafkaParams = mutable.Map[String, Object]()
    newKafkaParams ++= kafkaParams
    newKafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")

    //kafka api
    val consumer = new KafkaConsumer[String, Array[Byte]](newKafkaParams)
    //订阅
    consumer.subscribe(topics)
    val noOffsetForPartitionExceptionSet = mutable.Set()
    try {
      consumer.poll(0)
    } catch {
      case e: NoOffsetForPartitionException =>
      //        noOffsetForPartitionExceptionSet.add(e.partition())
      //邮件报警
    }
    //获取 分区信息
    val topicp = consumer.assignment().toSet
    //暂定消费
    consumer.pause(topicp)
    //从尾开始
    consumer.seekToEnd(topicp)
    val toMap = topicp.map(line => line -> consumer.position(line)).toMap
    val earliestOffsetMap = toMap
    consumer.unsubscribe()
    consumer.close()
    earliestOffsetMap
  }


  /**
   * 持久化偏移量
   *
   * @param rdd
   * @param storeOffset ：--> 可插拔参数
   * @tparam K
   * @tparam V
   */
  def persistOffset[K, V](rdd: RDD[ConsumerRecord[K, V]], storeOffset: Boolean = true) = {

    val groupId = kafkaParams("group.id").toString
    //rdd强转成OffsetRanges
    val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

    offsetRanges.foreach(or => {
      val zKGroupTopicDirs = new ZKGroupTopicDirs(groupId, or.topic)
      //拼装路径
      val offsetPath = zKGroupTopicDirs.consumerOffsetDir + "/" + or.partition

      /**
       * 可插拔参数
       * fromOffset Inclusive starting offset
       * untilOffset Exclusive ending offset
       */
      val data = if (storeOffset) or.untilOffset else or.fromOffset
      //提交偏移量
      zkUtils.updatePersistentPath(offsetPath, data + "")
    })


  }
}
