import java.util.Date

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.utils.DateUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Duration, Minutes, Seconds, StreamingContext}

import scala.collection.mutable.ArrayBuffer

object AdverStat {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("adver").setMaster("local[*]")
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    // val streamingContext = StreamingContext.getActiveOrCreate(checkpointDir, func)
    val streamingContext = new StreamingContext(sparkSession.sparkContext, Seconds(5))

    val kafka_brokers: String = ConfigurationManager.config.getString(Constants.KAFKA_BROKERS)
    val kafka_topics: String = ConfigurationManager.config.getString(Constants.KAFKA_TOPICS)

    val kafkaParam = Map(
      "bootstrap.servers" -> kafka_brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "group1",
      // auto.offset.reset
      // latest：先去Zookeeper获取offset，如果有，直接使用，如果没有，从最新的数据开始消费
      // earlist：先去Zookeeper获取offset，如果有，直接使用，如果没有，从最开始的数据开始消费
      // none：先去Zookeeper获取offset，如果有，直接使用，如果没有，直接报错
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    // adRealTimeDStream: InputDStream[RDD] RDD[message] message: key value
    val adRealTimeDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      // 定位策略
      LocationStrategies.PreferConsistent,
      // 消费策略：指定topic和kafka配置参数
      ConsumerStrategies.Subscribe[String, String](Array(kafka_topics), kafkaParam)
    )

    // 取出DStream里面每一条数据的value值
    // adRealTimeValueDStream: DStream[RDD] RDD[String]
    // String: timestamp province city userid adid
    val adRealTimeValueDStream: DStream[String] = adRealTimeDStream.map(item => item.value())
    val adRealTimeFilteredDStream: DStream[String] = adRealTimeValueDStream.transform {
      logRDD =>
        // 查询出黑名单表中的数据
        // AdBlacklist: userid
        val blackListArray: Array[AdBlacklist] = AdBlacklistDAO.findAll()

        // 取出黑名单表中的userid
        // userIdArray: Array[Long] [userid1, userid2, ...]
        val userIdArray: Array[Long] = blackListArray.map(item => item.userid)

        // 过滤掉黑名单表的userId，返回新的logRDD
        logRDD.filter {
          // log: timestamp province city userid adid
          case log =>
            val logSplit: Array[String] = log.split(" ")
            val userid: Long = logSplit(3).toLong
            !userIdArray.contains(userid)
        }
    }

    // adRealTimeFilteredDStream.foreachRDD(rdd => rdd.foreach(println(_)))

    streamingContext.checkpoint("./spark-streaming")

    adRealTimeFilteredDStream.checkpoint(Duration(10000))

    // 需求一：实时维护黑名单
    generateBlackList(adRealTimeFilteredDStream)

    // 需求二：各省各城市一天中的广告点击量（累积统计）
    val key2ProvinceCityCountDStream: DStream[(String, Long)] = provinceCityClickStat(adRealTimeFilteredDStream)

    // 需求三：统计各省Top3热门广告
    provinceTop3Adver(sparkSession, key2ProvinceCityCountDStream)

    // 需求四：最近一个小时广告点击量统计
    getRecentHourClickCount(adRealTimeFilteredDStream)

    streamingContext.start()
    streamingContext.awaitTermination()
  }


  def generateBlackList(adRealTimeFilteredDStream: DStream[String]) = {
    // adRealTimeFilteredDStream: DStream[RDD[String]] String -> log: timestamp province city userid adid
    // key2NumDStream: [RDD[(key, 1L)]]
    val key2NumDStream: DStream[(String, Long)] = adRealTimeFilteredDStream.map {
      case log =>
        val logSplit: Array[String] = log.split(" ")
        val timestamp: Long = logSplit(0).toLong
        // yyyyMMdd
        val dateKey: String = DateUtils.formatDateKey(new Date(timestamp))
        val userid: Long = logSplit(3).toLong
        val adid: Long = logSplit(4).toLong

        val key: String = dateKey + "_" + userid + "_" + adid

        (key, 1L)
    }

    val key2CountDStream: DStream[(String, Long)] = key2NumDStream.reduceByKey(_ + _)
    // 根据每一个RDD里面的数据，更新用户点击次数表
    key2CountDStream.foreachRDD {
      rdd =>
        rdd.foreachPartition {
          items =>
            val clickCountArray: ArrayBuffer[AdUserClickCount] = new ArrayBuffer[AdUserClickCount]()

            for ((key, count) <- items) {
              val keySplit: Array[String] = key.split("_")
              val date: String = keySplit(0)
              val userid: Long = keySplit(1).toLong
              val adid: Long = keySplit(2).toLong

              clickCountArray += AdUserClickCount(date, userid, adid, count)
            }

            AdUserClickCountDAO.updateBatch(clickCountArray.toArray)
        }
    }

    // key2BlackListDStream: DStream[RDD[(key, count)]]
    val key2BlackListDStream: DStream[(String, Long)] = key2CountDStream.filter {
      case (key, count) =>
        val keySplit: Array[String] = key.split("_")
        val date: String = keySplit(0)
        val userid: Long = keySplit(1).toLong
        val adid: Long = keySplit(2).toLong

        val clickCount: Int = AdUserClickCountDAO.findClickCountByMultiKey(date, userid, adid)

        if (clickCount > 100) {
          true
        } else {
          false
        }
    }

    // userIdDStream: DStream[RDD[userid]]
    val userIdDStream: DStream[Long] = key2BlackListDStream.map {
      // 将dateKey_userId_adId转换为userId并对userId去重
      case (key, count) => key.split("_")(1).toLong
    }.transform(rdd => rdd.distinct())

    userIdDStream.foreachRDD {
      rdd =>
        rdd.foreachPartition {
          items =>
            val useridArray: ArrayBuffer[AdBlacklist] = new ArrayBuffer[AdBlacklist]()

            for (userid <- items) {
              useridArray += AdBlacklist(userid)
            }

            AdBlacklistDAO.insertBatch(useridArray.toArray)
        }
    }
  }

  def provinceCityClickStat(adRealTimeFilteredDStream: DStream[String]) = {
    // adRealTimeFilteredDStream: DStream[RDD[String]] String -> log: timestamp province city userid adid
    // key2ProvinceCityDStream: DStream[RDD[(key, 1L)]]
    val key2ProvinceCityDStream: DStream[(String, Long)] = adRealTimeFilteredDStream.map {
      case log =>
        val logSplit: Array[String] = log.split(" ")
        val timestamp: Long = logSplit(0).toLong
        val dateKey: String = DateUtils.formatDateKey(new Date(timestamp))
        val province: String = logSplit(1)
        val city: String = logSplit(2)
        val adid: String = logSplit(4)

        val key: String = dateKey + "_" + province + "_" + city + "_" + adid
        (key, 1L)
    }

    // key2StateDStream: 某一天一个省的一个城市中某一个广告的点击次数（累积）
    val key2StateDStream: DStream[(String, Long)] = key2ProvinceCityDStream.updateStateByKey[Long] {
      (values: Seq[Long], state: Option[Long]) =>
        var newValue: Long = 0L
        if (state.isDefined)
          newValue = state.get
        for (value <- values) {
          newValue += value
        }
        Some(newValue)
    }

    key2StateDStream.foreachRDD {
      rdd => rdd.foreachPartition {
        items =>
          val adStatArray: ArrayBuffer[AdStat] = new ArrayBuffer[AdStat]()
          // key: date province city adId
          for ((key, count) <- items) {
            val keySplit: Array[String] = key.split("_")
            val date: String = keySplit(0)
            val province: String = keySplit(1)
            val city: String = keySplit(2)
            val adid: Long = keySplit(3).toLong

            adStatArray += AdStat(date, province, city, adid, count)
          }
          AdStatDAO.updateBatch(adStatArray.toArray)
      }
    }

    key2StateDStream
  }

  def provinceTop3Adver(sparkSession: SparkSession,
                        key2ProvinceCityCountDStream: DStream[(String, Long)]) = {
    // key2ProvinceCityCountDStream: [RDD[(key, count)]]
    // key: date_province_city_adid
    // key2ProvinceCountDStream: [RDD[(newKey, count)]]
    // newKey: date_province_adid
    val key2ProvinceCountDStream: DStream[(String, Long)] = key2ProvinceCityCountDStream.map {
      case (key, count) =>
        val keySplit: Array[String] = key.split("_")
        val date: String = keySplit(0)
        val province: String = keySplit(1)
        val adid: String = keySplit(3)

        val newKey: String = date + "_" + province + "_" + adid
        (newKey, count)
    }

    val key2ProvinceAggrCountDStream: DStream[(String, Long)] = key2ProvinceCountDStream.reduceByKey(_ + _)

    val top3DStream: DStream[Row] = key2ProvinceAggrCountDStream.transform {
      rdd =>
        // rdd: RDD[(key, count)]
        // key: date_province_adid
        val basicDataRDD: RDD[(String, String, Long, Long)] = rdd.map {
          case (key, count) =>
            val keySplit: Array[String] = key.split("_")
            val date: String = keySplit(0)
            val province: String = keySplit(1)
            val adid: Long = keySplit(2).toLong

            (date, province, adid, count)
        }

        import sparkSession.implicits._
        basicDataRDD.toDF("date", "province", "adid", "count")
          .createOrReplaceTempView("tmp_basic_info")

        val sql: String = "select date, province, adid, count from (" +
          "select date, province, adid, count, " +
          "row_number() over(partition by date, province order by count desc) rank from tmp_basic_info) t " +
          "where rank <= 3"

        sparkSession.sql(sql).rdd
    }

    top3DStream.foreachRDD {
      // rdd: RDD[Row]
      rdd =>
        rdd.foreachPartition {
          // items: Row
          items =>
            val top3Array: ArrayBuffer[AdProvinceTop3] = new ArrayBuffer[AdProvinceTop3]()
            for (item <- items) {
              val date: String = item.getAs[String]("date")
              val province: String = item.getAs[String]("province")
              val adid: Long = item.getAs[Long]("adid")
              val count: Long = item.getAs[Long]("count")

              top3Array += AdProvinceTop3(date, province, adid, count)
            }
            AdProvinceTop3DAO.updateBatch(top3Array.toArray)
        }
    }
  }

  def getRecentHourClickCount(adRealTimeFilteredDStream: DStream[String]) = {
    val key2TimeMinuteDStream: DStream[(String, Long)] = adRealTimeFilteredDStream.map {
      // log: timestamp province userid adid
      case log =>
        val logSplit: Array[String] = log.split(" ")
        val timestamp: Long = logSplit(0).toLong
        // yyyyMMddHHmm
        val timeMinute: String = DateUtils.formatTimeMinute(new Date(timestamp))
        val adid: Long = logSplit(4).toLong

        val key: String = timeMinute + "_" + adid

        (key, 1L)
    }

    val key2WindowDStream: DStream[(String, Long)] = key2TimeMinuteDStream.reduceByKeyAndWindow((a: Long, b: Long) => (a + b), Minutes(60), Minutes(1))

    key2WindowDStream.foreachRDD {
      rdd => rdd.foreachPartition {
        // (key, count)
        items =>
          val trendArray: ArrayBuffer[AdClickTrend] = new ArrayBuffer[AdClickTrend]()
          for ((key, count) <- items) {
            val keySplit: Array[String] = key.split("_")
            // yyyyMMddHHmm
            val timeMinute: String = keySplit(0)
            val date: String = timeMinute.substring(0, 8)
            val hour: String = timeMinute.substring(8, 10)
            val minute: String = timeMinute.substring(10)
            val adid: Long = keySplit(1).toLong

            trendArray += AdClickTrend(date, hour, minute, adid, count)
          }
          AdClickTrendDAO.updateBatch(trendArray.toArray)
      }
    }
  }

}
