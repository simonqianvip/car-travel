package com.cartravel.spark

import com.cartravel.common.MailUtil
import com.cartravel.kafka.KafkaManager
import com.cartravel.loggings.Logging
import com.cartravel.util.JedisUtil
import com.cartravel.utils.{DataStruct, GlobalConfigUtils}
import org.apache.commons.lang3.StringUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.scheduler.{BatchInfo, StreamingListener, StreamingListenerBatchCompleted, StreamingListenerBatchStarted, StreamingListenerBatchSubmitted, StreamingListenerOutputOperationCompleted, StreamingListenerReceiverError, StreamingListenerReceiverStarted, StreamingListenerReceiverStopped}
import org.json4s.DefaultFormats
import org.json4s.jackson.Json
import redis.clients.jedis.Jedis

class StreamingAppListener(
                          session:SparkSession,
                          conf:SparkConf,
                          duration:Int,
                          appName:String,
                          kafkaManager:KafkaManager,
                          rdd:RDD[ConsumerRecord[String, String]]
                          ) extends StreamingListener with Logging{

  var map = Map[String,String]()

  private val currentTime: Long = System.currentTimeMillis()

  private val jedis: Jedis = JedisUtil.getInstance().getJedis

  jedis.select(2)

  override def onReceiverStarted(receiverStarted: StreamingListenerReceiverStarted): Unit = super.onReceiverStarted(receiverStarted)

  override def onReceiverStopped(receiverStopped: StreamingListenerReceiverStopped): Unit = super.onReceiverStopped(receiverStopped)

  /**
   * 接受到错误信息发送邮件
   * @param receiverError
   */
  override def onReceiverError(receiverError: StreamingListenerReceiverError): Unit = {

    logger.info("----> onReceiverError method <----")

    val lastError = receiverError.receiverInfo.lastError
    val lastErrorMessage = receiverError.receiverInfo.lastErrorMessage

    if(StringUtils.isNotEmpty(lastError)){
        if(conf.getBoolean("enableSendEmailOnTaskFail",false)){
          val args = Array(GlobalConfigUtils.getProp("mail.host"), s"SparkStreaming监控任务:${appName}_${lastError}", lastErrorMessage)

          val prop = DataStruct.convertProp(
            ("mail.smtp.auth", GlobalConfigUtils.getProp("mail.smtp.auth")),
            ("mail.transport.protocol", GlobalConfigUtils.getProp("mail.transport.protocol"))
          )
          MailUtil.sendMail(prop,args)
        }
      Thread.interrupted()
    }
  }

  /**
   * 调度时间
   * @param batchStarted
   */
  override def onBatchStarted(batchStarted: StreamingListenerBatchStarted): Unit = {
    super.onBatchStarted(batchStarted)
    logger.info("----> onBatchStarted <----")

    val schedulingDelay: String = batchStarted.batchInfo.schedulingDelay.get.toString
    map += ("schedulingDelay"->schedulingDelay)

  }

  /**
   * 当前批次的数据
   * @param batchSubmitted
   */
  override def onBatchSubmitted(batchSubmitted: StreamingListenerBatchSubmitted): Unit = {
    super.onBatchSubmitted(batchSubmitted)
    logger.info("----> onBatchSubmitted <----")
    val numRecords: String = batchSubmitted.batchInfo.numRecords.toString
    map += ("numRecords"-> numRecords)
  }

  override def onOutputOperationCompleted(outputOperationCompleted: StreamingListenerOutputOperationCompleted): Unit = {
    super.onOutputOperationCompleted(outputOperationCompleted)
    logger.info("----> onOutputOperationCompleted <----")
    val duration = outputOperationCompleted.outputOperationInfo.duration.get.toString
    val failureReason = outputOperationCompleted.outputOperationInfo.failureReason.get

    map +=("duration"->duration)
  }

  /**
   * 当前批次执行成功回调改方法
   * @param batchCompleted
   */
  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {

    logger.info("----> onBatchCompleted <----")
    super.onBatchCompleted(batchCompleted)
    kafkaManager.persistOffset(rdd)

    val batchInfo: BatchInfo = batchCompleted.batchInfo


    val totalDelay = batchInfo.totalDelay.get.toString
    map += ("totalDelay"->totalDelay)

    val StreamingListenerApp = s"StreamingAppListener_${currentTime}"

    jedis.set("StreamingListenerApp",Json(DefaultFormats).write(map))
    jedis.expire("StreamingListenerApp",3600)

    val processingEndTime = batchInfo.processingEndTime
    val processingStartTime = batchInfo.processingStartTime
    val processingDelay = batchInfo.processingDelay

    if(duration * 6 < totalDelay.toLong * 1000){
      val monitorTitle = s"SparkStreaming ${appName}出现阻塞"
      val monitorContent =
        s"""
          |StreamingListener:
          |总耗费时间：${totalDelay},
          |开始时间：${processingStartTime},
          |结束时间：${processingEndTime},
          |处理时间：${processingDelay},
          |请及时处理
          |""".stripMargin

      if(conf.getBoolean("enableSendEmailOnTaskFail",false)){
        val args = Array(GlobalConfigUtils.getProp("mail.host"), s"${monitorTitle}", monitorContent)

        val prop = DataStruct.convertProp(
          ("mail.smtp.auth", GlobalConfigUtils.getProp("mail.smtp.auth")),
          ("mail.transport.protocol", GlobalConfigUtils.getProp("mail.transport.protocol"))
        )
        MailUtil.sendMail(prop,args)
      }


    }

  }
}
