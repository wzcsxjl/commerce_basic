import java.util.{Date, Random, UUID}

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.model.{UserInfo, UserVisitAction}
import commons.utils._
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
  * Session占比统计
  */
object SessionStat {

  def main(args: Array[String]): Unit = {
    // 获取筛选条件
    val jsonStr: String = ConfigurationManager.config.getString(Constants.TASK_PARAMS)
    // 将json串转换为JSONObject对象
    val taskParam: JSONObject = JSONObject.fromObject(jsonStr)
    // 创建全局唯一的主键
    val taskUUID: String = UUID.randomUUID().toString
    // 创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("session").setMaster("local[*]")
    // 创建SparkSession（包含SparkContext）
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    // 获取原始的动作表数据
    val actionRDD: RDD[UserVisitAction] = getOriActionRDD(sparkSession, taskParam)
    // actionRDD.foreach(println)

    // 将动作表数据格式转换为(sessionId, UserVisitAction)，以sessionId为K，UserVisitAction为V
    // sessionId2ActionRDD: RDD[(sessionId, UserVisitAction)]
    val sessionId2ActionRDD: RDD[(String, UserVisitAction)] = actionRDD.map(item => (item.session_id, item))

    // 按K（sessionId）进行分组
    // session2GroupActionRDD: RDD[(sessionId, Iterable[UserVisitAction])]
    val session2GroupActionRDD: RDD[(String, Iterable[UserVisitAction])] = sessionId2ActionRDD.groupByKey()
    // 将前面的计算结果缓存
    // sparkSession.sparkContext.setCheckpointDir("./")
    session2GroupActionRDD.cache()
    // session2GroupActionRDD.checkpoint()
    // session2GroupActionRDD.foreach(println(_))

    // 获取聚合数据里面的聚合信息
    // sessionId2FullInfoRDD: RDD[(sessionId, fullInfo)]
    val sessionId2FullInfoRDD: RDD[(String, String)] = getSessionFullInfo(sparkSession, session2GroupActionRDD)
    // sessionId2FullInfoRDD.foreach(println)

    // 创建自定义累加器对象
    val sessionStatisticAccumulator: SessionStatisticAccumulator = new SessionStatisticAccumulator
    // 注册自定义累加器
    sparkSession.sparkContext.register(sessionStatisticAccumulator)
    // 得到所有符合过滤条件的数据组成的RDD
    // sessionId2FilteredRDD: RDD[(sessionId, fullInfo)]
    val sessionId2FilteredRDD: RDD[(String, String)] = getSessionFilteredRDD(taskParam,
      sessionId2FullInfoRDD,
      sessionStatisticAccumulator)
    // 如果没有最后的行动算子那么就不会执行计算
    // sessionId2FilteredRDD.foreach(println)
    sessionId2FilteredRDD.count()
    // 获取最终的Session占比统计结果
    // taskUUID为写入MySQL数据库的唯一主键
    getSessionRatio(sparkSession, taskUUID, sessionStatisticAccumulator.value)

    // 需求二：session随机抽取
    // sessionId2FilteredRDD: RDD[(sid, fullInfo)]，一个session对应一条数据，即一个fullInfo
    sessionRandomExtract(sparkSession, taskUUID, sessionId2FilteredRDD)

    // sessionId2ActionRDD: RDD[(sessionId, UserVisitAction)]
    // sessionId2FilteredRDD: RDD[(sessionId, fullInfo)]
    // sessionId2FilteredActionRDD: RDD[sessionId, UserVisitAction]
    // 获取所有符合过滤条件的action数据
    val sessionId2FilteredActionRDD: RDD[(String, UserVisitAction)] = sessionId2ActionRDD.join(sessionId2FilteredRDD)
      .map {
      case (sessionId, (action, fullInfo)) =>
        (sessionId, action)
    }

    // 需求三：Top10热门品类统计
    // top10CategoryArray: Array[(SortKey, countInfo)]
    val top10CategoryArray: Array[(SortKey, String)] = top10PopularCategories(sparkSession,
      taskUUID,
      sessionId2FilteredActionRDD)

    // 需求四：Top10热门品类的Top10活跃session统计
    top10ActiveSession(sparkSession, taskUUID, sessionId2FilteredActionRDD, top10CategoryArray)
  }

  /**
    * 获取原始动作表数据
    *
    * @param sparkSession
    * @param taskParam
    * @return
    */
  def getOriActionRDD(sparkSession: SparkSession, taskParam: JSONObject): RDD[UserVisitAction] = {
    // 获取限制条件的startDate
    val startDate: String = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE)
    val endDate: String = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE)
    val sql: String = "select * from user_visit_action where date >= '" + startDate + "' and date <= '" + endDate + "'"
    // 隐式转换
    import sparkSession.implicits._
    // DF转换为DS再返回RDD
    sparkSession.sql(sql).as[UserVisitAction].rdd
  }

  /**
    * 获取sessionId对应的聚合信息，将userId作为K，联立user_info表拼接user_info表中的相关字段，最后返回(sessionId, fullInfo)
    *
    * @param sparkSession
    * @param session2GroupActionRDD
    * @return
    */
  def getSessionFullInfo(sparkSession: SparkSession,
                         session2GroupActionRDD: RDD[(String, Iterable[UserVisitAction])]): RDD[(String, String)] = {
    // userId2AggrInfoRDD: RDD[(userId, aggrInfo)]
    val userId2AggrInfoRDD: RDD[(Long, String)] = session2GroupActionRDD.map {
      case (sessionId, iterableAction) =>
        var userId: Long = -1L

        var startTime: Date = null
        var endTime: Date = null

        // 访问步长：session中的action个数（一个session中有多少条数据）
        var stepLength: Int = 0

        val searchKeywords: StringBuffer = new StringBuffer("")
        val clickCategories: StringBuffer = new StringBuffer("")

        for (action <- iterableAction) {
          if (userId == -1L) {
            userId = action.user_id
          }
          // 将Sting类型的action_time转换为Date类型
          val actionTime: Date = DateUtils.parseTime(action.action_time)
          // 当startTime为空或者startTime在actionTime之后时将actionTime赋值给startTime
          if (startTime == null || startTime.after(actionTime)) {
            startTime = actionTime
          }
          if (endTime == null || endTime.before(actionTime)) {
            endTime = actionTime
          }
          val search_keyword: String = action.search_keyword
          // 当search_keyword不为空并且searchKeywords中不包含search_keyword时，将search_keyword拼接到searchKeywords中
          if (StringUtils.isNotEmpty(search_keyword) && !searchKeywords.toString.contains(search_keyword)) {
            searchKeywords.append(search_keyword + ",")
          }
          val clickCategoryId: Long = action.click_category_id
          if (clickCategoryId != -1 && !clickCategories.toString.contains(clickCategoryId)) {
            clickCategories.append(clickCategoryId + ",")
          }
          stepLength += 1
        }

        // 去掉最后面拼接的逗号
        // 相当于searchKeywords.toString.substring(0, searchKeywords.toString.length - 1)
        val searchKw: String = StringUtils.trimComma(searchKeywords.toString)
        val clickCg: String = StringUtils.trimComma(clickCategories.toString)

        val visitLength: Long = (endTime.getTime - startTime.getTime) / 1000

        val aggrInfo: String = Constants.FIELD_SESSION_ID + "=" + sessionId + "|" +
          Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKw + "|" +
          Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCg + "|" +
          Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|" +
          Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|" +
          Constants.FIELD_START_TIME + "=" + DateUtils.formatTime(startTime)

        (userId, aggrInfo)
    }

    val sql: String = "select * from user_info"
    import sparkSession.implicits._
    val userId2InfoRDD: RDD[(Long, UserInfo)] = sparkSession.sql(sql).as[UserInfo].rdd
      .map(item => (item.user_id, item))

    val sessionId2FullInfoRDD: RDD[(String, String)] = userId2AggrInfoRDD.join(userId2InfoRDD).map {
      case (userId, (aggrInfo, userInfo)) =>
        val age: Int = userInfo.age
        val professional: String = userInfo.professional
        val sex: String = userInfo.sex
        val city: String = userInfo.city

        val fullInfo: String = aggrInfo + "|" +
          Constants.FIELD_AGE + "=" + age + "|" +
          Constants.FIELD_PROFESSIONAL + "=" + professional + "|" +
          Constants.FIELD_SEX + "=" + sex + "|" +
          Constants.FIELD_CITY + "=" + city

        // 取出sessionId
        val sessionId: String = StringUtils
          .getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_SESSION_ID)

        (sessionId, fullInfo)
    }

    sessionId2FullInfoRDD
  }

  /**
    * 实现根据限制条件对session数据进行过滤，并完成累加器的更新
    *
    * @param taskParam
    * @param sessionId2FullInfoRDD
    * @param sessionStatisticAccumulator
    * @return
    */
  def getSessionFilteredRDD(taskParam: JSONObject,
                            sessionId2FullInfoRDD: RDD[(String, String)],
                            sessionStatisticAccumulator: SessionStatisticAccumulator) = {
    val startAge: String = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE)
    val endAge: String = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE)
    val professionals: String = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS)
    val cities: String = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES)
    val sex: String = ParamUtils.getParam(taskParam, Constants.PARAM_SEX)
    val keywords: String = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS)
    val categoryIds: String = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS)

    var filterInfo: String = (if (startAge != null) Constants.PARAM_START_AGE + "=" + startAge + "|" else "") +
      (if (endAge != null) Constants.PARAM_END_AGE + "=" + endAge + "|" else "") +
      (if (professionals != null) Constants.PARAM_PROFESSIONALS + "=" + professionals + "|" else "") +
      (if (cities != null) Constants.PARAM_CITIES + "=" + cities + "|" else "") +
      (if (sex != null) Constants.PARAM_SEX + "=" + sex + "|" else "") +
      (if (keywords != null) Constants.PARAM_KEYWORDS + "=" + keywords + "|" else "") +
      (if (categoryIds != null) Constants.PARAM_CATEGORY_IDS + "=" + categoryIds else "")

    if (filterInfo.endsWith("\\|"))
      filterInfo = filterInfo.substring(0, filterInfo.length - 1)

    sessionId2FullInfoRDD.filter {
      case (sessionId, fullInfo) =>
        var success: Boolean = true
        if (!ValidUtils
          .between(fullInfo, Constants.FIELD_AGE, filterInfo, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE)) {
          success = false
        } else if (!ValidUtils.in(fullInfo, Constants.FIELD_PROFESSIONAL, filterInfo, Constants.PARAM_PROFESSIONALS)) {
          success = false
        } else if (!ValidUtils.in(fullInfo, Constants.FIELD_CITY, filterInfo, Constants.PARAM_CITIES)) {
          success = false
        } else if (!ValidUtils.equal(fullInfo, Constants.FIELD_SEX, filterInfo, Constants.PARAM_SEX)) {
          success = false
        } else if (!ValidUtils.in(fullInfo, Constants.FIELD_SEARCH_KEYWORDS, filterInfo, Constants.PARAM_KEYWORDS)) {
          success = false
        } else if (!ValidUtils
          .in(fullInfo, Constants.FIELD_CLICK_CATEGORY_IDS, filterInfo, Constants.PARAM_CATEGORY_IDS)) {
          success = false
        }

        if (success) {
          sessionStatisticAccumulator.add(Constants.SESSION_COUNT)
          val visitLength: Long = StringUtils
            .getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_VISIT_LENGTH).toLong
          val stepLength: Long = StringUtils
            .getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_STEP_LENGTH).toLong
          calculateVisitLength(visitLength, sessionStatisticAccumulator)
          calculateStepLength(stepLength, sessionStatisticAccumulator)
        }
        success
    }
  }

  /**
    * 计算各范围内的访问时长数量
    *
    * @param visitLength
    * @param sessionStatisticAccumulator
    */
  def calculateVisitLength(visitLength: Long,
                           sessionStatisticAccumulator: SessionStatisticAccumulator) = {
    if (visitLength >= 1 && visitLength <= 3) {
      sessionStatisticAccumulator.add(Constants.TIME_PERIOD_1s_3s)
    } else if (visitLength >= 4 && visitLength <= 6) {
      sessionStatisticAccumulator.add(Constants.TIME_PERIOD_4s_6s)
    } else if (visitLength >= 7 && visitLength <= 9) {
      sessionStatisticAccumulator.add(Constants.TIME_PERIOD_7s_9s)
    } else if (visitLength >= 10 && visitLength <= 30) {
      sessionStatisticAccumulator.add(Constants.TIME_PERIOD_10s_30s)
    } else if (visitLength > 30 && visitLength <= 60) {
      sessionStatisticAccumulator.add(Constants.TIME_PERIOD_30s_60s)
    } else if (visitLength > 60 && visitLength <= 180) {
      sessionStatisticAccumulator.add(Constants.TIME_PERIOD_1m_3m)
    } else if (visitLength > 180 && visitLength <= 600) {
      sessionStatisticAccumulator.add(Constants.TIME_PERIOD_3m_10m)
    } else if (visitLength > 600 && visitLength <= 1800) {
      sessionStatisticAccumulator.add(Constants.TIME_PERIOD_10m_30m)
    } else if (visitLength > 1800) {
      sessionStatisticAccumulator.add(Constants.TIME_PERIOD_30m)
    }
  }

  /**
    * 计算各范围内访问步长的数量
    *
    * @param stepLength
    * @param sessionStatisticAccumulator
    */
  def calculateStepLength(stepLength: Long,
                          sessionStatisticAccumulator: SessionStatisticAccumulator) = {
    if (stepLength >= 1 && stepLength <= 3) {
      sessionStatisticAccumulator.add(Constants.STEP_PERIOD_1_3)
    } else if (stepLength >= 4 && stepLength <= 6) {
      sessionStatisticAccumulator.add(Constants.STEP_PERIOD_4_6)
    } else if (stepLength >= 7 && stepLength <= 9) {
      sessionStatisticAccumulator.add(Constants.STEP_PERIOD_7_9)
    } else if (stepLength >= 10 && stepLength <= 30) {
      sessionStatisticAccumulator.add(Constants.STEP_PERIOD_10_30)
    } else if (stepLength > 30 && stepLength <= 60) {
      sessionStatisticAccumulator.add(Constants.STEP_PERIOD_30_60)
    } else if (stepLength > 60) {
      sessionStatisticAccumulator.add(Constants.STEP_PERIOD_60)
    }
  }

  /**
    * 统计Session各范围访问步长、访问时长占比
    *
    * @param sparkSession
    * @param taskUUID
    * @param value
    */
  def getSessionRatio(sparkSession: SparkSession,
                      taskUUID: String,
                      value: mutable.HashMap[String, Int]): Unit = {
    // 总的session数量
    val session_count: Double = value.getOrElse(Constants.SESSION_COUNT, 1).toDouble

    val visit_length_1s_3s: Int = value.getOrElse(Constants.TIME_PERIOD_1s_3s, 0)
    val visit_length_4s_6s: Int = value.getOrElse(Constants.TIME_PERIOD_4s_6s, 0)
    val visit_length_7s_9s: Int = value.getOrElse(Constants.TIME_PERIOD_7s_9s, 0)
    val visit_length_10s_30s: Int = value.getOrElse(Constants.TIME_PERIOD_10s_30s, 0)
    val visit_length_30s_60s: Int = value.getOrElse(Constants.TIME_PERIOD_30s_60s, 0)
    val visit_length_1m_3m: Int = value.getOrElse(Constants.TIME_PERIOD_1m_3m, 0)
    val visit_length_3m_10m: Int = value.getOrElse(Constants.TIME_PERIOD_3m_10m, 0)
    val visit_length_10m_30m: Int = value.getOrElse(Constants.TIME_PERIOD_10m_30m, 0)
    val visit_length_30m: Int = value.getOrElse(Constants.TIME_PERIOD_30m, 0)

    val step_length_1_3: Int = value.getOrElse(Constants.STEP_PERIOD_1_3, 0)
    val step_length_4_6: Int = value.getOrElse(Constants.STEP_PERIOD_4_6, 0)
    val step_length_7_9: Int = value.getOrElse(Constants.STEP_PERIOD_7_9, 0)
    val step_length_10_30: Int = value.getOrElse(Constants.STEP_PERIOD_10_30, 0)
    val step_length_30_60: Int = value.getOrElse(Constants.STEP_PERIOD_30_60, 0)
    val step_length_60: Int = value.getOrElse(Constants.STEP_PERIOD_60, 0)

    val visit_length_1s_3s_ratio: Double = NumberUtils.formatDouble(visit_length_1s_3s / session_count, 2)
    val visit_length_4s_6s_ratio: Double = NumberUtils.formatDouble(visit_length_4s_6s / session_count, 2)
    val visit_length_7s_9s_ratio: Double = NumberUtils.formatDouble(visit_length_7s_9s / session_count, 2)
    val visit_length_10s_30s_ratio: Double = NumberUtils.formatDouble(visit_length_10s_30s / session_count, 2)
    val visit_length_30s_60s_ratio: Double = NumberUtils.formatDouble(visit_length_30s_60s / session_count, 2)
    val visit_length_1m_3m_ratio: Double = NumberUtils.formatDouble(visit_length_1m_3m / session_count, 2)
    val visit_length_3m_10m_ratio: Double = NumberUtils.formatDouble(visit_length_3m_10m / session_count, 2)
    val visit_length_10m_30m_ratio: Double = NumberUtils.formatDouble(visit_length_10m_30m / session_count, 2)
    val visit_length_30m_ratio: Double = NumberUtils.formatDouble(visit_length_30m / session_count, 2)

    val step_length_1_3_ratio: Double = NumberUtils.formatDouble(step_length_1_3 / session_count, 2)
    val step_length_4_6_ratio: Double = NumberUtils.formatDouble(step_length_4_6 / session_count, 2)
    val step_length_7_9_ratio: Double = NumberUtils.formatDouble(step_length_7_9 / session_count, 2)
    val step_length_10_30_ratio: Double = NumberUtils.formatDouble(step_length_10_30 / session_count, 2)
    val step_length_30_60_ratio: Double = NumberUtils.formatDouble(step_length_30_60 / session_count, 2)
    val step_length_60_ratio: Double = NumberUtils.formatDouble(step_length_60 / session_count, 2)

    val stat: SessionAggrStat = SessionAggrStat(taskUUID,
      session_count.toInt, visit_length_1s_3s_ratio, visit_length_4s_6s_ratio, visit_length_7s_9s_ratio,
      visit_length_10s_30s_ratio, visit_length_30s_60s_ratio, visit_length_1m_3m_ratio,
      visit_length_3m_10m_ratio, visit_length_10m_30m_ratio, visit_length_30m_ratio,
      step_length_1_3_ratio, step_length_4_6_ratio, step_length_7_9_ratio,
      step_length_10_30_ratio, step_length_30_60_ratio, step_length_60_ratio)

    val sessionRatioRDD: RDD[SessionAggrStat] = sparkSession.sparkContext.makeRDD(Array(stat))

    import sparkSession.implicits._
    // 将统计数据写入MySQL
    sessionRatioRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable", "session_stat_ratio")
      .mode(SaveMode.Append)
      .save()
  }

  /**
    * session随机抽取
    *
    * @param sparkSession
    * @param taskUUID
    * @param sessionId2FilteredRDD
    */
  def sessionRandomExtract(sparkSession: SparkSession,
                           taskUUID: String,
                           sessionId2FilteredRDD: RDD[(String, String)]) = {
    // 转换结构，将dateHour: yyyy-MM-dd_HH作为key
    // dateHour2FullInfoRDD: RDD[dateHour, fullInfo]
    val dateHour2FullInfoRDD: RDD[(String, String)] = sessionId2FilteredRDD.map {
      case (sid, fullInfo) =>
        // 从fullInfo中取出startTime
        val startTime: String = StringUtils
          .getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_START_TIME)
        // 将startTime格式变为yyyy-MM-dd_HH形式，便于后面使用"_"切分date和hour
        // dateHour: yyyy-MM-dd_HH
        val dateHour: String = DateUtils.getDateHour(startTime)
        (dateHour, fullInfo)
    }

    // 使用countByKey()计算每个小时的session总数
    // hourCountMap: collection.Map[dateHour, count]
    val hourCountMap: collection.Map[String, Long] = dateHour2FullInfoRDD.countByKey()

    // 转换为<yyyy-MM-dd, <HH, count>>的格式
    // dateHourCountMap: Map[(date, Map[(hour, count)])]
    val dateHourCountMap: mutable.HashMap[String, mutable.HashMap[String, Long]] = new mutable
    .HashMap[String, mutable.HashMap[String, Long]]()

    for ((dateHour, count) <- hourCountMap) {
      val date: String = dateHour.split("_")(0)
      val hour: String = dateHour.split("_")(1)
      dateHourCountMap.get(date) match {
        case None => dateHourCountMap(date) = new mutable.HashMap[String, Long]()
          dateHourCountMap(date) += (hour -> count)
        case Some(map) => dateHourCountMap(date) += (hour -> count)
      }
    }

    /*
    将抽取的数量根据天数平均分配到每天
    解决问题一：一共有多少天 dateHourCountMap.size
               一天抽取多少条 100 / dateHourCountMap.size
     */
    val extractPerDay: Long = 100 / dateHourCountMap.size

    /*
    解决问题二：一天有多少session dateHourCountMap(date).values.sum
    解决问题三：一个小时有多少session dateHourCountMap(date)(hour)
     */
    // dateHourExtractIndexListMap: Map[date, Map[hour, ListBuffer[index]]]
    val dateHourExtractIndexListMap: mutable.HashMap[String, mutable.HashMap[String, ListBuffer[Int]]] = new mutable
    .HashMap[String, mutable.HashMap[String, ListBuffer[Int]]]()

    // dateHourCountMap: Map[date, Map[hour, count])]
    for ((date, hourCountMap) <- dateHourCountMap) {
      // 一天有多少条session
      val dateSessionCount: Long = hourCountMap.values.sum

      // 得到每小时随机索引列表的Map集合
      // Map[hour, ListBuffer[index]
      dateHourExtractIndexListMap.get(date) match {
        case None => dateHourExtractIndexListMap(date) = new mutable.HashMap[String, ListBuffer[Int]]()
          generateRandomIndexList(extractPerDay, dateSessionCount, hourCountMap, dateHourExtractIndexListMap(date))
        case Some(map) =>
          generateRandomIndexList(extractPerDay, dateSessionCount, hourCountMap, dateHourExtractIndexListMap(date))
      }

      // 到目前为止，我们获得了每个小时要抽取的session的index
      // 广播大变量，提升任务性能
      // dateHourExtractIndexListMapBd: Map[date, Map[hour, ListBuffer[Index]]]
      val dateHourExtractIndexListMapBc: Broadcast[mutable.HashMap[String, mutable.HashMap[String, ListBuffer[Int]]]] = sparkSession
        .sparkContext.broadcast(dateHourExtractIndexListMap)

      // 使用groupByKey()算子将每小时的session访问数据进行聚合
      // dateHour2FullInfoRDD: RDD[(dateHour, fullInfo)]
      // dateHour2GroupRDD: RDD[(dateHour, iterableFullInfo)]
      val dateHour2GroupRDD: RDD[(String, Iterable[String])] = dateHour2FullInfoRDD.groupByKey()
      // dateHour2GroupRDD.foreach(println(_))

      val extractSessionRDD: RDD[SessionRandomExtract] = dateHour2GroupRDD.flatMap {
        // dateHour: yyyy-MM-dd_HH
        case (dateHour, iterableFullInfo) =>
          val date: String = dateHour.split("_")(0)
          val hour: String = dateHour.split("_")(1)

          // 根据广播变量，对数据集中的数据进行抽取
          // 将广播变量中的索引列表取出来，得到extractList: ListBuffer[index]
          val extractList: ListBuffer[Int] = dateHourExtractIndexListMapBc.value.get(date).get(hour)

          /*
          SessionRandomExtract(taskid:String,
                                sessionid:String,
                                startTime:String,
                                searchKeywords:String,
                                clickCategoryIds:String)
           */
          val extractSessionArrayBuffer: ArrayBuffer[SessionRandomExtract] = new ArrayBuffer[SessionRandomExtract]()

          // 初始化iterableFullInfo的索引，从0开始
          var index = 0

          // 对每小时中所有的fullInfo循环遍历
          for (fullInfo <- iterableFullInfo) {
            // 判断随机索引列表中是否包含当前索引
            if (extractList.contains(index)) {
              val sessionId: String = StringUtils
                .getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_SESSION_ID)
              val startTime: String = StringUtils
                .getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_START_TIME)
              val searchKeywords: String = StringUtils
                .getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_SEARCH_KEYWORDS)
              val clickCategories: String = StringUtils
                .getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_CLICK_CATEGORY_IDS)

              // 将包含在随机索引列表中的fullInfo对应字段赋值给extractSession
              val extractSession: SessionRandomExtract = SessionRandomExtract(taskUUID,
                sessionId, startTime, searchKeywords, clickCategories)
              // 将extractSession添加到SessionRandomExtract类型的ArrayBuffer中
              extractSessionArrayBuffer += extractSession
            }
            // iterableFullInfo的索引累加
            index += 1
          }

          extractSessionArrayBuffer
      }

      import sparkSession.implicits._
      extractSessionRDD.toDF().write
        .format("jdbc")
        .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
        .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
        .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
        .option("dbtable", "session_extract")
        .mode(SaveMode.Append)
        .save()
    }
  }

  /**
    * 生成随机索引列表
    *
    * @param extractPerDay
    * @param dateSessionCount
    * @param hourCountMap
    * @param hourExtractIndexListMap
    */
  def generateRandomIndexList(extractPerDay: Long,
                              dateSessionCount: Long,
                              hourCountMap: mutable.HashMap[String, Long],
                              hourExtractIndexListMap: mutable.HashMap[String, ListBuffer[Int]]) = {
    for ((hour, count) <- hourCountMap) {
      /*
      按一天中每小时session条数占比计算出每小时将要抽取的session条数
      获取一个小时要抽取多少条数据
      (每小时session条数 / 一天session条数) * 每天的抽取条数
       */
      var hourExtCount: Int = ((count / dateSessionCount.toDouble) * extractPerDay).toInt
      // 避免一个小时要抽取的数量超过这个小时的总数
      if (hourExtCount > count) {
        hourExtCount = count.toInt
      }

      val random = new Random()
      // 获取每小时的随机索引列表
      // hourExtractIndexListMap: Map[hour, ListBuffer[index]]
      hourExtractIndexListMap.get(hour) match {
        // 第一次进来时ListBuffer[index]为空，先实例化一个ListBuffer[Int]
        case None => hourExtractIndexListMap(hour) = new ListBuffer[Int]
          // 在每小时要抽取的session条数范围内循环
          for (i <- 0 until hourExtCount) {
            // 在每小时总的session条数范围内产生一个随机数
            var index: Int = random.nextInt(count.toInt)
            // 判断随机索引列表中是否包含产生的随机数
            while (hourExtractIndexListMap(hour).contains(index)) {
              // 如何包含则再随机生成一个
              index = random.nextInt(count.toInt)
            }
            // 将随机数追加到随机索引列表中
            hourExtractIndexListMap(hour).append(index)
          }
        case Some(list) =>
          for (i <- 0 until hourExtCount) {
            var index = random.nextInt(count.toInt)
            while (hourExtractIndexListMap(hour).contains(index)) {
              index = random.nextInt(count.toInt)
            }
            hourExtractIndexListMap(hour).append(index)
          }
      }
    }
  }

  /**
    * 获取top10热门品类
    * @param sparkSession
    * @param taskUUID
    * @param sessionId2FilteredActionRDD
    * @return
    */
  def top10PopularCategories(sparkSession: SparkSession,
                             taskUUID: String,
                             sessionId2FilteredActionRDD: RDD[(String, UserVisitAction)]) = {
    // 第一步：获取所有发生过点击、下单、付款的品类
    // 使用flatMap()算子，将点击、下单、付款行为中的品类id取出并转换为cid2CidRDD: RDD[(cid, cid)]结构
    var cid2CidRDD: RDD[(Long, Long)] = sessionId2FilteredActionRDD.flatMap {
      case (sid, action) =>
        val categoryBuffer: ArrayBuffer[(Long, Long)] = new ArrayBuffer[(Long, Long)]()
        // 点击行为
        if (action.click_category_id != -1) {
          categoryBuffer += ((action.click_category_id, action.click_category_id))
        } else if (action.order_category_ids != null) {
          // 下单行为，每次下单可以有多个品类，使用","分隔
          for (orderCid <- action.order_category_ids.split(","))
            categoryBuffer += ((orderCid.toLong, orderCid.toLong))
        } else if (action.pay_category_ids != null) {
          // 下单行为，每次下单可以有多个品类，使用","分隔
          for (payCid <- action.pay_category_ids.split(","))
            categoryBuffer += ((payCid.toLong, payCid.toLong))
        }
        categoryBuffer
    }
    // 去除重复的cid
    cid2CidRDD = cid2CidRDD.distinct()

    // 第二步：统计品类的点击次数，下单次数、付款次数
    val cid2ClickCountRDD: RDD[(Long, Long)] = getClickCount(sessionId2FilteredActionRDD)
    val cid2OrderCountRDD: RDD[(Long, Long)] = getOrderCount(sessionId2FilteredActionRDD)
    val cid2PayCountRDD: RDD[(Long, Long)] = getPayCount(sessionId2FilteredActionRDD)
    // 根据cid进行关联得到每个品类对应的点击、下单和付款次数
    // cid2FullCountRDD: RDD[(cid, countInfo)]
    // (62,categoryid=62|clickCount=69|orderCount=99|payCount=75)
    val cid2FullCountRDD: RDD[(Long, String)] = getFullCount(cid2CidRDD,
      cid2ClickCountRDD,
      cid2OrderCountRDD,
      cid2PayCountRDD)
    // cid2FullCountRDD.foreach(println)
    // 实现自定义二次排序key
    val sortKey2FullCountRDD: RDD[(SortKey, String)] = cid2FullCountRDD.map {
      case (cid, countInfo) =>
        // 取出需要排序的点击、下单和付款次数
        val clickCount: Long = StringUtils
          .getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_CLICK_COUNT).toLong
        val orderCount: Long = StringUtils
          .getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_ORDER_COUNT).toLong
        val payCount: Long = StringUtils
          .getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_PAY_COUNT).toLong
        // 放入自定义排序样例类中
        val sortKey: SortKey = SortKey(clickCount, orderCount, payCount)
        (sortKey, countInfo)
    }
    // 根据点击、下单、付款次数倒序排列并取前10
    val top10CategoryArray: Array[(SortKey, String)] = sortKey2FullCountRDD.sortByKey(false)
      .take(10)
    // 整理结构
    val top10CategoryRDD: RDD[Top10Category] = sparkSession.sparkContext.makeRDD(top10CategoryArray).map {
      case (sortKey, countInfo) =>
      val cid: Long = StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_CATEGORY_ID)
        .toLong
        val clickCount: Long = sortKey.clickCount
        val orderCount: Long = sortKey.orderCount
        val payCount: Long = sortKey.payCount
        /*case class Top10Category(taskid:String,
                                 categoryid:Long,
                                 clickCount:Long,
                                 orderCount:Long,
                                 payCount:Long)*/
        Top10Category(taskUUID, cid, clickCount, orderCount, payCount)
    }
    import sparkSession.implicits._
    // 写入MySQL数据库
    top10CategoryRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable", "top10_category")
      .mode(SaveMode.Append)
      .save()
    top10CategoryArray
  }

  /**
    * 获取品类点击次数
    *
    * @param sessionId2FilteredActionRDD
    * @return
    */
  def getClickCount(sessionId2FilteredActionRDD: RDD[(String, UserVisitAction)]) = {
    /*
    先进行过滤，保留点击行为对应的action
    sessionId2FilteredActionRDD.filter {
       case (sessionId, action) => action.click_category_id != -1L
    }
    */
    val clickFilteredRDD: RDD[(String, UserVisitAction)] = sessionId2FilteredActionRDD
      .filter(item => item._2.click_category_id != -1L)
    // 转换格式，为reduceByKey()做准备
    // clickFilteredRDD.map {case (sessionId, action) => (action.click_category_id, 1L)}
    val clickNumRDD: RDD[(Long, Long)] = clickFilteredRDD.map(item => (item._2.click_category_id, 1L))
    // 计算点击次数
    clickNumRDD.reduceByKey(_ + _)
  }

  /**
    * 获取品类下单次数
    *
    * @param sessionId2FilteredActionRDD
    * @return
    */
  def getOrderCount(sessionId2FilteredActionRDD: RDD[(String, UserVisitAction)]) = {
    val orderFilteredRDD: RDD[(String, UserVisitAction)] = sessionId2FilteredActionRDD
      .filter(item => item._2.order_category_ids != null)
    val orderNumRDD: RDD[(Long, Long)] = orderFilteredRDD.flatMap {
      // 先将字符串拆分为字符串数组，再使用map转化数组中的每个元素
      // 将数组中的每一个string元素，转化为(long, 1L)
      // action.order_category_ids.split(","): Array[String]
      case (sessionId, action) => action.order_category_ids.split(",").map(item => (item.toLong, 1L))
    }
    orderNumRDD.reduceByKey(_ + _)
  }

  /**
    * 获取品类付款次数
    * @param sessionId2FilteredActionRDD
    * @return
    */
  def getPayCount(sessionId2FilteredActionRDD: RDD[(String, UserVisitAction)]) = {
    val payFilteredRDD: RDD[(String, UserVisitAction)] = sessionId2FilteredActionRDD
      .filter(item => item._2.pay_category_ids != null)
    val payNumRDD: RDD[(Long, Long)] = payFilteredRDD.flatMap {
      case (sid, action) => action.pay_category_ids.split(",").map(item => (item.toLong, 1L))
    }
    payNumRDD.reduceByKey(_ + _)
  }

  /**
    * RDD集合做leftJoin操作，得到RDD[(cid, categoryid=?|clickCount=?|orderCount=?|payCount=?)]
    * @param cid2CidRDD
    * @param cid2ClickCountRDD
    * @param cid2OrderCountRDD
    * @param cid2PayCountRDD
    * @return
    */
  def getFullCount(cid2CidRDD: RDD[(Long, Long)],
                   cid2ClickCountRDD: RDD[(Long, Long)],
                   cid2OrderCountRDD: RDD[(Long, Long)],
                   cid2PayCountRDD: RDD[(Long, Long)]) = {
    val cid2ClickInfoRDD: RDD[(Long, String)] = cid2CidRDD.leftOuterJoin(cid2ClickCountRDD).map {
      case (cid, (categoryId, option)) =>
        val clickCount: Long = if (option.isDefined) option.get else 0
        val aggrInfo: String = Constants.FIELD_CATEGORY_ID + "=" + cid + "|" +
          Constants.FIELD_CLICK_COUNT + "=" + clickCount
        (cid, aggrInfo)
    }
    val cid2OrderInfoRDD: RDD[(Long, String)] = cid2ClickInfoRDD.leftOuterJoin(cid2OrderCountRDD).map {
      case (cid, (clickInfo, option)) =>
        val orderCount: Long = if (option.isDefined) option.get else 0
        val aggrInfo: String = clickInfo + "|" + Constants.FIELD_ORDER_COUNT + "=" + orderCount
        (cid, aggrInfo)
    }
    val cid2PayInfoRDD: RDD[(Long, String)] = cid2OrderInfoRDD.leftOuterJoin(cid2PayCountRDD).map {
      case (cid, (orderInfo, option)) =>
        val payCount: Long = if (option.isDefined) option.get else 0
        val aggrInfo: String = orderInfo + "|" + Constants.FIELD_PAY_COUNT + "=" + payCount
        (cid, aggrInfo)
    }
    cid2PayInfoRDD
  }

  def top10ActiveSession(sparkSession: SparkSession,
                         taskUUID: String,
                         sessionId2FilteredActionRDD: RDD[(String, UserVisitAction)],
                         top10CategoryArray: Array[(SortKey, String)]) = {
    // 第一步：过滤出所有点击过Top10品类的action
    // 方法一：使用join操作
    /*val cid2CountInfoRDD: RDD[(Long, String)] = sparkSession.sparkContext.makeRDD(top10CategoryArray).map {
      case (sortKey, countInfo) =>
        val cid: Long = StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_CATEGORY_ID).toLong
        (cid, countInfo)
    }
    val cid2ActionRDD: RDD[(Long, UserVisitAction)] = sessionId2FilteredActionRDD.map {
      case (sessionId, action) =>
        val cid: Long = action.click_category_id
        (cid, action)
    }
    val sessionId2ActionRDD: RDD[(String, UserVisitAction)] = cid2CountInfoRDD.join(cid2ActionRDD).map {
      case (cid, (countInfo, action)) =>
        val sid: String = action.session_id
        (sid, action)
    }*/

    // 方法二：使用filter算子
    // cidArray: Array[Long]
    val cidArray: Array[Long] = top10CategoryArray.map {
      case (sortKey, countInfo) =>
        val cid: Long = StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_CATEGORY_ID)
          .toLong
        cid
    }
    // 取出所有符合过滤条件的，并且点击过Top10热门品类的action
    /*val sessionId2ActionRDD: RDD[(String, UserVisitAction)] = sessionId2FilteredActionRDD.filter {
      case (sessionId, action) =>
        cidArray.contains(action.click_category_id)
    }*/
    val sessionId2ActionRDD: RDD[(String, UserVisitAction)] = sessionId2FilteredActionRDD
      .filter(item => cidArray.contains(item._2.click_category_id))
    // 按照sessionId进行聚合
    // sessionId2GroupRDD: RDD[(cid, sid=clickCount)]
    val sessionId2GroupRDD: RDD[(String, Iterable[UserVisitAction])] = sessionId2ActionRDD.groupByKey()
    val cid2SessionCountRDD: RDD[(Long, String)] = sessionId2GroupRDD.flatMap {
      case (sessionId, iterableAction) =>
        val categoryCountMap: mutable.HashMap[Long, Long] = new mutable.HashMap[Long, Long]
        for (action <- iterableAction) {
          val cid: Long = action.click_category_id
          if (!categoryCountMap.contains(cid))
            categoryCountMap += (cid -> 0)
          categoryCountMap.update(cid, categoryCountMap(cid) + 1)
        }
        // 记录了一个session对应所有点击过的品类的点击次数
        // 使用yield关键字将遍历过程中处理的结果返回到一个新Vector集合中
        // cid2SidCount: Map[cid, sid=clickCount]
        val cid2SidCount: mutable.HashMap[Long, String] = for ((cid, count) <- categoryCountMap)
          yield (cid, sessionId + "=" + count)
        cid2SidCount
    }
    // cid2GroupRDD每一条数据都是一个categoryId对应所有点击过它的session对它的点击次数
    // cid2GroupRDD: RDD[(cid, IterableSessionCount)]
    val cid2GroupRDD: RDD[(Long, Iterable[String])] = cid2SessionCountRDD.groupByKey()
    val top10SessionRDD: RDD[Top10Session] = cid2GroupRDD.flatMap {
      case (cid, iterableSessionCount) =>
        // true: item1放在前面
        // false: item2放在前面
        // item: sessionCount String "sessionId=count"
        // 对每个cid对应的所有session点击次数倒序排列并取前10
        // sortList: List[sessionCount]
        val sortList: List[String] = iterableSessionCount.toList.sortWith((item1, item2) => {
          item1.split("=")(1).toLong > item2.split("=")(1).toLong
        }).take(10)
        val top10Session: List[Top10Session] = sortList.map {
          case item =>
            val sessionId: String = item.split("=")(0)
            val count: Long = item.split("=")(1).toLong
            Top10Session(taskUUID, cid, sessionId, count)
        }
        top10Session
    }
    import sparkSession.implicits._
    top10SessionRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable", "top10_session")
      .mode(SaveMode.Append)
      .save
  }

}
