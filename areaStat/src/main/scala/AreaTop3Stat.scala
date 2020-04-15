import java.util.UUID

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.utils.ParamUtils
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

object AreaTop3Stat {

  def main(args: Array[String]): Unit = {
    val jsonStr: String = ConfigurationManager.config.getString(Constants.TASK_PARAMS)
    val taskParam: JSONObject = JSONObject.fromObject(jsonStr)

    val taskUUID: String = UUID.randomUUID().toString

    val sparkConf: SparkConf = new SparkConf().setAppName("area").setMaster("local[*]")
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    // cityId2PidRDD: RDD[(cityId, pid)]
    val cityId2PidRDD: RDD[(Long, Long)] = getCityAndProductInfo(sparkSession, taskParam)

    // RDD[(cityId, CityAreaInfo)]
    val cityId2AreaInfoRDD: RDD[(Long, CityAreaInfo)] = getCityAreaInfo(sparkSession)

    // tmp_area_basic_info：表中的一条数据代表一次点击商品行为
    getAreaPidBasicInfoTable(sparkSession, cityId2PidRDD, cityId2AreaInfoRDD)
    // sparkSession.sql("select * from tmp_area_basic_info").show()

    sparkSession.udf.register("concat_long_string", (v1: Long, v2: String, split: String) => {
      v1 + split + v2
    })

    sparkSession.udf.register("group_concat_distinct", new GroupConcatDistinct)

    getAreaProductClickCountTable(sparkSession)
    // sparkSession.sql("select * from tmp_area_click_count").show()

    sparkSession.udf.register("get_json_field", (json: String, field: String) => {
      val jSONObject: JSONObject = JSONObject.fromObject(json)
      jSONObject.getString(field)
    })

    getAreaProductClickCountInfo(sparkSession)
    // sparkSession.sql("select * from tmp_area_count_product_info").show()

    getTop3Product(sparkSession, taskUUID)
    // sparkSession.sql("select * from temp_test").show()
  }

  def getCityAndProductInfo(sparkSession: SparkSession, taskParam: JSONObject) = {
    val startDate: String = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE)
    val endDate: String = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE)
    // 只获取发生过点击的action数据
    // 获取到的一条action数据代表一个点击行为
    val sql: String = "select city_id, click_product_id from user_visit_action where date >= '" +
      startDate + "' and date <= '" + endDate + "' and click_product_id != -1"
    import sparkSession.implicits._
    // case class CityClickProduct(city_id:Long, click_product_id:Long)
    /*sparkSession.sql(sql).as[CityClickProduct].rdd.map {
      case cityClickProduct => (cityClickProduct.city_id, cityClickProduct.click_product_id)
    }*/
    sparkSession.sql(sql).as[CityClickProduct].rdd.map(item => (item.city_id, item.click_product_id))
  }

  def getCityAreaInfo(sparkSession: SparkSession) = {
    // Array[(cityId, cityName, area)]
    val cityAreaInfoArray: Array[(Long, String, String)] = Array((0L, "北京", "华北"), (1L, "上海", "华东"),
      (2L, "南京", "华东"), (3L, "广州", "华南"), (4L, "三亚", "华南"), (5L, "武汉", "华中"),
      (6L, "长沙", "华中"), (7L, "西安", "西北"), (8L, "成都", "西南"), (9L, "哈尔滨", "东北"))
    /*sparkSession.sparkContext.makeRDD(cityAreaInfoArray).map {
      case (cityId, cityName, area) =>
        (cityId, CityAreaInfo(cityId, cityName, area))
    }*/
    // RDD[(cityId, CityAreaInfo)]
    sparkSession.sparkContext.makeRDD(cityAreaInfoArray).map(item => (item._1, CityAreaInfo(item._1, item._2, item._3)))
  }

  /**
    * 创建基本信息表
    *
    * @param sparkSession
    * @param cityId2PidRDD
    * @param cityId2AreaInfoRDD
    */
  def getAreaPidBasicInfoTable(sparkSession: SparkSession,
                               cityId2PidRDD: RDD[(Long, Long)],
                               cityId2AreaInfoRDD: RDD[(Long, CityAreaInfo)]) = {
    val areaPidInfoRDD: RDD[(Long, String, String, Long)] = cityId2PidRDD.join(cityId2AreaInfoRDD).map {
      case (cityId, (pid, areaInfo)) =>
        (cityId, areaInfo.city_name, areaInfo.area, pid)
    }

    import sparkSession.implicits._
    areaPidInfoRDD.toDF("city_id", "city_name", "area", "pid")
      .createOrReplaceTempView("tmp_area_basic_info")
  }

  def getAreaProductClickCountTable(sparkSession: SparkSession) = {
    val sql = "select area, pid, count(*) click_count," +
      " group_concat_distinct(concat_long_string(city_id, city_name, ':')) city_infos" +
      " from tmp_area_basic_info group by area, pid"

    sparkSession.sql(sql).createOrReplaceTempView("tmp_area_click_count")
  }

  def getAreaProductClickCountInfo(sparkSession: SparkSession) = {
    val sql = "select tacc.area, tacc.city_infos, tacc.pid, pi.product_name, " +
      "if(get_json_field(pi.extend_info, 'product_status') = '0', 'Self', 'Third Party') product_status, " +
      "tacc.click_count from tmp_area_click_count tacc join product_info pi on tacc.pid = pi.product_id"

    sparkSession.sql(sql).createOrReplaceTempView("tmp_area_count_product_info")
  }

  def getTop3Product(sparkSession: SparkSession, taskUUID: String) = {
    /*val sql = "select area, city_infos, pid, product_name, product_status, click_count, " +
      "row_number() over(partition by area order by click_count desc) rank from tmp_area_count_product_info"

    sparkSession.sql(sql).createOrReplaceTempView("temp_test")*/

    val sql = "select area, " +
      "case " +
      "when area = '华北' or area = '华东' then 'A_Level' " +
      "when area = '华中' or area = '华南' then 'B_Level' " +
      "when area = '西南' or area = '西北' then 'C_Level' " +
      "else 'D_Level' " +
      "end area_level, " +
      "city_infos, pid, product_name, product_status, click_count from (" +
      "select area, city_infos, pid, product_name, product_status, click_count, " +
      "row_number() over(partition by area order by click_count DESC) rank from tmp_area_count_product_info) t " +
      "where rank <= 3"

    val top3ProductRDD: RDD[AreaTop3Product] = sparkSession.sql(sql).rdd.map {
      case row =>
        AreaTop3Product(taskUUID, row.getAs[String]("area"), row.getAs[String]("area_level"),
          row.getAs[Long]("pid"), row.getAs[String]("city_infos"),
          row.getAs[Long]("click_count"), row.getAs[String]("product_name"),
          row.getAs[String]("product_status"))
    }

    import sparkSession.implicits._
    top3ProductRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("dbtable", "area_top3_product")
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .mode(SaveMode.Append)
      .save()
  }

}
