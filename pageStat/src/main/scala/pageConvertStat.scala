import java.util.UUID

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.model.UserVisitAction
import commons.utils.{DateUtils, ParamUtils}
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable


object pageConvertStat {

  def main(args: Array[String]): Unit = {
    // 获取任务限制条件
    val jsonStr: String = ConfigurationManager.config.getString(Constants.TASK_PARAMS)
    val taskParam: JSONObject = JSONObject.fromObject(jsonStr)
    // 获取唯一主键
    val taskUUID: String = UUID.randomUUID().toString
    // 创建sparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("pageConvert").setMaster("local[*]")
    // 创建sparkSession
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    // 获取用户行为数据
    val sessionId2ActionRDD: RDD[(String, UserVisitAction)] = getUserVisitAction(sparkSession, taskParam)
    // sessionId2ActionRDD.foreach(println)
    // targetPageFlow:"1,2,3,4,5,6,7"
    // pageFlowStr: "1,2,3,4,5,6,7"
    val pageFlowStr: String = ParamUtils.getParam(taskParam, Constants.PARAM_TARGET_PAGE_FLOW)
    // pageFlowArray: Array(1,2,3,4,5,6,7)
    // 拿到访问页面路径
    val pageFlowArray: Array[String] = pageFlowStr.split(",")
    // pageFlowArray.slice(0, pageFlowArray.length - 1): Array(1, 2, 3, 4, 5, 6)
    // pageFlowArray.tail: Array(2, 3, 4, 5, 6, 7)
    // pageFlowArray.slice(0, pageFlowArray.length - 1).zip(pageFlowArray.tail): Array((1,2), (2,3), (3,4), (4,5)...(6,7))
    // targetPageSplit: Array(1_2, 2_3, 3_4, ..., 6_7)
    /*val targetPageSplit = pageFlowArray.slice(0, pageFlowArray.length - 1).zip(pageFlowArray.tail).map {
      case (page1, page2) => page1 + "_" + page2
    }*/
    // 符合业务对象的单跳标签
    val targetPageSplit: Array[String] = pageFlowArray.slice(0, pageFlowArray.length - 1).zip(pageFlowArray.tail)
      .map(item => item._1 + "_" + item._2)
    // sessionId2ActionRDD: RDD[(sessionId, IterableAction)]
    val sessionId2GroupRDD: RDD[(String, Iterable[UserVisitAction])] = sessionId2ActionRDD.groupByKey()
    // pageSplitNumRDD: RDD[pageSplit, 1L] 每个sessionId符合条件的pageSplit
    // 如[(2_3, 1), (5_6, 1)]
    val pageSplitNumRDD: RDD[(String, Long)] = sessionId2GroupRDD.flatMap {
      // 以sessionId为维度进行计算
      case (sessionId, iterableAction) =>
        // item1: userVisitAction
        // item2: userVisitAction
        val sortList: List[UserVisitAction] = iterableAction.toList.sortWith((item1, item2) => {
          DateUtils.parseTime(item1.action_time).getTime < DateUtils.parseTime(item2.action_time).getTime
        })
        // pageList: List[page_id] 如：List(1, 5, 6, 2, 7，4，3)
        val pageList: List[Long] = sortList.map(item => item.page_id)
        // pageList.slice(0, pageList.length - 1): List(1, 5, 6, 2, 7，4)
        // pageList.tail: List[page_id] List(5, 6, 2, 7，4，3)
        // pageSplit: List[String] List(1_5, 5_6, 6_2, ...)
        val pageSplit: List[String] = pageList.slice(0, pageList.length - 1).zip(pageList.tail).map {
          case (page1, page2) => page1 + "_" + page2
        }
        // 单个session符合页面单跳的数据
        // 过滤出符合条件(1_2, 2_3, 3_4, ..., 6_7)的页面切片
        // pageSplit.filter(pageSplit => targetPageSplit.contains(pageSplit))
        val pageSplitFiltered: List[String] = pageSplit.filter(targetPageSplit.contains(_))
        /*pageSplitFiltered.map {
          case pageSplit => (pageList, 1L)
        }*/
        // (5_6, 1)
        pageSplitFiltered.map((_, 1L))
    }
    // 计算出所有页面切片的数量 Map("1_2" -> count1, "2_3" -> count2, ...)
    // pageSplitCountMap: Map[pageSplit, count]
    val pageSplitCountMap: collection.Map[String, Long] = pageSplitNumRDD.countByKey()

    // 拿到首页路径
    val startPage: Long = pageFlowArray(0).toLong
    /*sessionId2ActionRDD.filter {
      case (sessionId, action) => action.page_id == startPage
    }.count()*/
    // 计算出访问首页的sessionId数，即首页有多少用户访问 count(1)
    val startPageCount: Long = sessionId2ActionRDD.filter(item => item._2.page_id == startPage).count

    getPageConvert(sparkSession, taskUUID, targetPageSplit, startPageCount, pageSplitCountMap)
  }

  def getUserVisitAction(sparkSession: SparkSession, taskParam: JSONObject) = {
    val startDate: String = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE)
    val endDate: String = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE)
    val sql: String = "select * from user_visit_action where date >= '" + startDate + "' and date <= '" + endDate + "'"
    import sparkSession.implicits._
    sparkSession.sql(sql).as[UserVisitAction].rdd.map(item => (item.session_id, item))
  }

  def getPageConvert(sparkSession: SparkSession,
                     taskUUID: String, targetPageSplit: Array[String],
                     startPageCount: Long,
                     pageSplitCountMap: collection.Map[String, Long]) = {
    val pageSplitRatio: mutable.HashMap[String, Double] = new mutable.HashMap[String, Double]()
    var lastPageCount: Double = startPageCount.toDouble
    // 1_2, 2_3, 3_4, ...
    for (pageSplit <- targetPageSplit) {
      // 第一次循环：lastPageCount: page1   currentPageSplitCount: page1_page2 结果：Convert(1_2)
      val currentPageSplitCount: Double = pageSplitCountMap.get(pageSplit).get.toDouble
      val ratio: Double = currentPageSplitCount / lastPageCount
      pageSplitRatio.put(pageSplit, ratio)
      lastPageCount = currentPageSplitCount
    }
    val convertStr: String = pageSplitRatio.map {
      case (pageSplit, ratio) => pageSplit + "=" + ratio
    }.mkString("|")
    val pageSplit: PageSplitConvertRate = PageSplitConvertRate(taskUUID, convertStr)
    val pageSplitRatioRDD: RDD[PageSplitConvertRate] = sparkSession.sparkContext.makeRDD(Array(pageSplit))
    import sparkSession.implicits._
    pageSplitRatioRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("dbtable", "page_split_convert_rate")
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .mode(SaveMode.Append)
      .save()
  }

}
