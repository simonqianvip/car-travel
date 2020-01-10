package com.cartravel.spark

import com.cartravel.common.MailUtil
import com.cartravel.loggings.Logging
import com.cartravel.util.JedisUtil
import com.cartravel.utils.{DataStruct, GlobalConfigUtils}
import org.apache.spark.{SparkConf, TaskEndReason, TaskFailedReason, TaskKilledException}
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.json4s.DefaultFormats
import org.json4s.jackson.Json
import redis.clients.jedis.Jedis

import scala.collection.mutable

class SparkAppListener(conf:SparkConf) extends SparkListener with Logging{
  val defaultJedisConfig = "jedisConfig.properties"
  private val jedis: Jedis = JedisUtil.getInstance().getJedis

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    jedis.select(0)

    val currentTime = System.currentTimeMillis()

    //---------------------------------taskMetrics----------------------------------------
    val metrics = taskEnd.taskMetrics
    val task_metrics_map = mutable.HashMap(
      "executorDeserializeTime" -> metrics.executorDeserializeTime,
      "executorDeserializeCpuTime" -> metrics.executorDeserializeCpuTime,
      "executorRunTime" -> metrics.executorRunTime,
      "executorCpuTime" -> metrics.executorCpuTime,
      "resultSize" -> metrics.resultSize,
      "jvmGCTime" -> metrics.jvmGCTime,
      "resultSerializationTime" -> metrics.resultSerializationTime,
      "memoryBytesSpilled" -> metrics.memoryBytesSpilled,
      "diskBytesSpilled" -> metrics.diskBytesSpilled,
      "peakExecutionMemory" -> metrics.peakExecutionMemory,
      "updatedBlockStatuses" -> metrics.updatedBlockStatuses
    )
    val taskMetricsKey = s"taskMetrics_${currentTime}"

    jedis.set(taskMetricsKey,Json(DefaultFormats).write(task_metrics_map))
    jedis.expireAt(taskMetricsKey,3600)
    

    //---------------------------------shuffle Metrics----------------------------------------
    val readMetrics = metrics.shuffleReadMetrics
    val writeMetrics = metrics.shuffleWriteMetrics
    
    val shuffle_metrics_map = mutable.HashMap(
      "remoteBlocksFetched" -> readMetrics.remoteBlocksFetched,
      "localBlocksFetched" -> readMetrics.localBlocksFetched,
      "remoteBytesRead" -> readMetrics.remoteBytesRead,
      "localBytesRead" -> readMetrics.localBytesRead,
      "fetchWaitTime" -> readMetrics.fetchWaitTime,
      "recordsRead" -> readMetrics.recordsRead,
      "bytesWritten" -> writeMetrics.bytesWritten,
      "recordsWritten" -> writeMetrics.recordsWritten,
      "writeTime" -> writeMetrics.writeTime
    )

    val shuffleMetricsKey = s"shuffleMetrics_${currentTime}"

    jedis.set(shuffleMetricsKey,Json(DefaultFormats).write(shuffle_metrics_map))
    jedis.expireAt(shuffleMetricsKey,3600)

    //---------------------------------input output Metrics----------------------------------------
    val inputMetrics = metrics.inputMetrics
    val outputMetrics = metrics.outputMetrics

    val input_output_map = mutable.HashMap(
      "bytesRead" -> inputMetrics.bytesRead,
      "recordsRead" -> inputMetrics.recordsRead,
      "bytesWritten" -> outputMetrics.bytesWritten,
      "recordsWritten" -> outputMetrics.recordsWritten
    )

    val input_output_key = s"input_output_${currentTime}"
    jedis.set(input_output_key,Json(DefaultFormats).write(input_output_map))
    jedis.expireAt(input_output_key,3600)

    //--------------------------------- taskInfo ----------------------------------------
    val taskInfo = taskEnd.taskInfo

    val taskInfo_map = mutable.HashMap(
      "taskId" -> taskInfo.taskId,
      "host" -> taskInfo.host,
      "speculative" -> taskInfo.speculative,
      "failed" -> taskInfo.failed,
      "killed" -> taskInfo.killed,
      "running" -> taskInfo.running
    )

    val taskInfo_key = s"taskInfo_${currentTime}"
    jedis.set(taskInfo_key,Json(DefaultFormats).write(taskInfo_map))
    jedis.expire(taskInfo_key,3600)

    //--------------------------------- 邮件报警 ----------------------------------------

    if(taskInfo != null && taskEnd.stageAttemptId != -1){
      val reason: TaskEndReason = taskEnd.reason
      val msg = reason match {
        case kill: TaskKilledException => Some(kill.getMessage)
        case e:TaskFailedReason => Some(e.toErrorString)
        case e:Exception => Some(e.getMessage)
        case _ => None
      }
      if(msg.nonEmpty){
        if(conf.getBoolean("enableSendEmailOnTaskFail",false)){
          val args = Array(GlobalConfigUtils.getProp("mail.host"), s"Spark监控任务:${reason}", msg.get)

          val prop = DataStruct.convertProp(
            ("mail.smtp.auth", GlobalConfigUtils.getProp("mail.smtp.auth")),
            ("mail.transport.protocol", GlobalConfigUtils.getProp("mail.transport.protocol"))
          )
          MailUtil.sendMail(prop,args)
        }
      }
    }






  }

}
