import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

class SessionStatisticAccumulator extends AccumulatorV2[String, mutable.HashMap[String, Int]] {

  // 保存所有聚合数据
  val countMap: mutable.HashMap[String, Int] = new mutable.HashMap[String, Int]()

  // 判断是否为初始值
  override def isZero: Boolean = {
    countMap.isEmpty
  }

  // 复制累加器
  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Int]] = {
    val acc: SessionStatisticAccumulator = new SessionStatisticAccumulator
    // ++=: 表示对Map进行拼接
    // 让新的countMap和当前的countMap一模一样
    acc.countMap ++= this.countMap
    acc
  }

  // 重置累加器中的值
  override def reset(): Unit = {
    countMap.clear()
  }

  // 向累加器中添加另一个值
  override def add(v: String): Unit = {
    if (!this.countMap.contains(v))
      this.countMap += (v -> 0)
    this.countMap.update(v, countMap(v) + 1)
  }

  // 各个task的累加器进行合并的方法
  // 合并另一个类型相同的累加器
  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Int]]): Unit = {
    // 模式匹配，确定other和自定义累加器是同一个累加器
    other match {
      // 左折叠
      // (0 /: (1 to 100))(_ + _)
      // (0 /: (1 to 100)) {case (int1, int2) => int1 + int2}
      // (1 /: 100).foldLeft(0)
      // (this.countMap /: acc.countMap)
      case acc: SessionStatisticAccumulator => acc.countMap.foldLeft(this.countMap) {
        case (map, (k, v)) => map += (k -> (map.getOrElse(k, 0) + v))
      }
    }
  }

  // 获取累加器中的值
  override def value: mutable.HashMap[String, Int] = {
    this.countMap
  }

}
