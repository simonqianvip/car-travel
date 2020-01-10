package com.cartravel.spark

import java.util

import com.alibaba.fastjson.JSONArray
import com.cartravel.util.{HBaseUtil, MapUtil}
import com.uber.h3core.H3Core
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.mapreduce.Job
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.locationtech.jts.geom.{Point, Polygon}
import org.locationtech.jts.io.WKTReader

import scala.collection.JavaConversions._

/**
  * 离线统计虚拟车站
  */
object VirtualStationsProcessor {
  //1.创建h3实例
  val h3 = H3Core.newInstance

  //2.经纬度转换成hash值
  def locationToH3(lat: Double, lon: Double, res: Int): Long = {
    h3.geoToH3(lat, lon, res)
  }

  def main(args: Array[String]): Unit = {
    val HTAB_HAIKOU_ORDER = "HTAB_HAIKOU_ORDER"
    import org.apache.spark._
    //设置Spark程序在控制台中的日志打印级别
    Logger.getLogger("org").setLevel(Level.WARN)
    //local[*]使用本地模式运行，*表示内部会自动计算CPU核数，也可以直接指定运行线程数比如2，就是local[2]
    //表示使用两个线程来模拟spark集群
    val conf = new SparkConf().setAppName("Virtual-stations").setMaster("local[2]")

    val sparkSession = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    //自定义方法
    sparkSession.udf.register("locationToH3", locationToH3 _)

    val hbConf = HBaseConfiguration.create(sparkSession.sparkContext.hadoopConfiguration)
//    hbConf.set("hbase.zookeeper.quorum", "10.20.3.177,10.20.3.178,10.20.3.179")
    hbConf.set("hbase.zookeeper.quorum", "192.168.80.200,192.168.80.210,192.168.80.220")
    hbConf.set("hbase.zookeeper.property.clientPort", "2181")

    val sqlContext = sparkSession.sqlContext

    val districtList = new java.util.ArrayList[com.cartravel.util.District]();
    val districts: JSONArray = MapUtil.getDistricts("海口市", null);
    MapUtil.parseDistrictInfo(districts, null, districtList);

    //行政区域广播变量（spark开发优化的一个点）
    val districtsBroadcastVar = sparkSession.sparkContext.broadcast(districtList)

    //加载hbase 中的订单数据
    val order = HBaseLoader.loadData(hbConf, sqlContext, HTAB_HAIKOU_ORDER)
    println("订单表数据:")
    //上线的时候把这样代码删除掉，影响性能
    order.show()

    //注册临时视图
    order.createOrReplaceTempView("order");

    //没什么业务功能
    val groupRdd = order.rdd.groupBy(row => {
      row.getString(1)
    })

    //在sql语句中使用h3接口进行六边形栅格化
    val gridDf = sparkSession.sql(
      s"""
         |select
         |ORDER_ID,
         |CITY_ID,
         |STARTING_LNG,
         |STARTING_LAT,
         |locationToH3(STARTING_LAT,STARTING_LNG,12) as  h3code
         | from order
         |""".stripMargin
    )

    println("h3栅格化的数据:")
    gridDf.show()
    gridDf.createOrReplaceTempView("order_grid")

    //分组统计
    val groupCountDf = gridDf.groupBy("h3code").count().filter("count>=10")
    groupCountDf.show()

    //统计结果注册临时视图
    groupCountDf.createOrReplaceTempView("groupcount")

    //使用sql进行join操作,升序取出最小精度，最小维度的点作为虚拟车站的经纬度位置信息
    val groupJoinDf = sparkSession.sql(
      s"""
         |select
         |ORDER_ID,
         |CITY_ID,
         |STARTING_LNG,
         |STARTING_LAT,
         |row_number() over(partition by order_grid.h3code order by STARTING_LNG,STARTING_LAT asc) rn
         | from order_grid join groupcount on order_grid.h3code = groupcount.h3code
         |having(rn=1)
         |""".stripMargin)

    println("==============================================")
    groupJoinDf.show()

    //判断经纬度在哪个行政区域，得到经纬度和行政区域名称的关联关系
    val groupJoinRdd = groupJoinDf.rdd
    val districtsRdd = groupJoinRdd.mapPartitions(rows => {
      var tmpRows = new java.util.ArrayList[Row]()
      import org.geotools.geometry.jts.JTSFactoryFinder
      val geometryFactory = JTSFactoryFinder.getGeometryFactory(null)
      var reader = new WKTReader(geometryFactory)

      //获取广播变量
      val districtList = districtsBroadcastVar.value
//      println("districtList:" + districtList)
      val wktPolygons = districtList.map(district => {
        val polygonStr = district.getPolygon
        var wktPolygon = ""
        if (!StringUtils.isEmpty(polygonStr)) {
          wktPolygon = "POLYGON((" + polygonStr.replaceAll(",", " ").replaceAll(";", ",") + "))"
          val polygon: Polygon = reader.read(wktPolygon).asInstanceOf[Polygon]
          (district, polygon)
        } else {
          null
        }
      }).filter(null != _)

      while (rows.hasNext) {
        val row: Row = rows.next()

        val lng = row.getAs[String]("STARTING_LNG")
        val lat = row.getAs[String]("STARTING_LAT")

        val wktPoint = "POINT(" + lng + " " + lat + ")";
        val point: Point = reader.read(wktPoint).asInstanceOf[Point];
        wktPolygons.foreach(polygon => {
          //判断经纬度点是否在行政区内
          if (polygon._2.contains(point)) {
            val fields = row.toSeq.toArray ++ Seq(polygon._1.getName)
            tmpRows.add(Row.fromSeq(fields))
          }
        })
      }
      Iterator(tmpRows)
    }).flatMap(rows => rows) //扁平化

    //构造新的schema
    var newSchema = groupJoinDf.schema
    //schema中添加新的列
    newSchema = newSchema.add("DISTRICT_NAME", "string")
    //构造新的Df
    val districtsDf = sparkSession.createDataFrame(districtsRdd, newSchema)

    println("虚拟车站数据:")
//    districtsDf.show()

    districtsDf.show(5)
    //保存统计数据
    HBaseLoader.saveOrWriteData(hbConf, districtsDf, "VIRTUAL_STATIONS")
  }
}

/**
  * Hbase表数据加载器
  */
object HBaseLoader {


  def loadData(configuration: Configuration, sc: SparkContext, tableName: String):
                                        RDD[(ImmutableBytesWritable, Result)] = {

    val scanner = new Scan

    //ORDER_ID String,CITY_ID String,STARTING_LNG String,STARTING_LAT String
    scanner.addFamily(Bytes.toBytes("f1"))
    scanner.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("ORDER_ID"))
    scanner.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("CITY_ID"))
    scanner.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("STARTING_LNG"))
    scanner.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("STARTING_LAT"))

    import org.apache.hadoop.hbase.protobuf.ProtobufUtil
    val proto = ProtobufUtil.toScan(scanner)

    val scanToString = Base64.encodeBytes(proto.toByteArray)

    import org.apache.hadoop.hbase.mapreduce.TableInputFormat
    //设置hbase表
    configuration.set(TableInputFormat.INPUT_TABLE, tableName)
    configuration.set(TableInputFormat.SCAN, scanToString)

    sc.newAPIHadoopRDD(configuration, classOf[TableInputFormat],
      classOf[ImmutableBytesWritable], classOf[Result])
  }

  def loadData(configuration: Configuration, sqlContext: SQLContext, tableName: String): DataFrame = {
    val sc = sqlContext.sparkContext
    val value: RDD[(ImmutableBytesWritable, Result)] = loadData(configuration, sc: SparkContext, tableName: String)

    //代码优化的点建议多实用mapPartitions,foreachPartitions
    val rowRDD = value.mapPartitions(iterator => {
      val newItems = new util.ArrayList[Row]()
      var row: Row = null;
      while (iterator.hasNext) {
        val result = iterator.next()._2
        row = Row.fromSeq(Seq(
          Bytes.toString(result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("ORDER_ID"))),
          Bytes.toString(result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("CITY_ID"))),
          Bytes.toString(result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("STARTING_LNG"))),
          Bytes.toString(result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("STARTING_LAT")))))
        newItems.add(row)
      }
      Iterator(newItems)
    }).flatMap(row => row)//扁平化的东西

    //field里面指定的类型要和Row里面的类型保持一致
    val fields = Array(
      new StructField("ORDER_ID", StringType),
      new StructField("CITY_ID", StringType),
      new StructField("STARTING_LNG", StringType),
      new StructField("STARTING_LAT", StringType)
    )

    val structType = new StructType(fields)
    sqlContext.createDataFrame(rowRDD, structType)
  }

  /**
    * 保存hbase数据
    *
    * @param configuration
    */
  def saveOrWriteData(configuration: Configuration, result: DataFrame, tableName: String): Unit = {
    configuration.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    val job = new Job(configuration);

    //ImmutableBytesWritable理解为hbase表中的rowkey（ImmutableBytesWritable,Result）
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

    val kvRdd = result.rdd.mapPartitions(iterator => {
      val newItems = new util.ArrayList[(ImmutableBytesWritable, Put)]()
      while (iterator.hasNext) {
        val next:Row = iterator.next()
        val put = new Put(Bytes.toBytes(next.getString(0)))
        //1.设置订单ID
        put.addColumn(
          Bytes.toBytes("f1"),
          Bytes.toBytes("ORDER_ID"),
          Bytes.toBytes(next.getAs[String]("ORDER_ID")))

        //2.设置城市id
        put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("CITY_ID"),
          Bytes.toBytes(next.getAs[String]("CITY_ID")))

        //3.设置经度
        put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("STARTING_LNG"),
          Bytes.toBytes(next.getAs[String]("STARTING_LNG")))

        //4.设置维度
        put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("STARTING_LAT"),
          Bytes.toBytes(next.getAs[String]("STARTING_LAT")))

        //5.设置行政区域名
        put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("DISTRICT_NAME"),
          Bytes.toBytes(next.getAs[String]("DISTRICT_NAME")))

        //6.添加到集合中
        newItems.add((new ImmutableBytesWritable, put))
      }
      Iterator(newItems)
    }).flatMap(kv => kv)

    HBaseUtil.createTable(HBaseUtil.getConnection, tableName, "f1")
    kvRdd.saveAsNewAPIHadoopDataset(job.getConfiguration)
  }
}
