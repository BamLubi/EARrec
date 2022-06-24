
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.fpm.FPGrowth
import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer

class Rule2(_his: ArrayBuffer[String], _item: String, _conf: Double) {
  var item: String = _item
  val his: ArrayBuffer[String] = _his
  var conf: Double = _conf
}

object WordCount {
  var min_sup: Double = 0.092 // 最小支持度阈值
  var D_size: Long = 0 // 购物车大小

  val base_path = "hdfs://127.0.0.1:9000/earrec/"
  val basket_path = "sample/trans_10W.txt"
  val user_path = "sample/test_2W.txt"
  val pattern_path = "out/pattern.txt"
  val res_path = "out/result.txt"

  def main(args: Array[String]): Unit ={

    println("Main Start ...")

    val conf = new SparkConf() // 创建SparkConf对象
    conf.setAppName("EARrec App")
    conf.setMaster("local[*]")

    val sc = new SparkContext(conf)

    // 1. 加载购物篮数据集
    println("Loading Basket Datasets from " + basket_path + " ...")
    val basket = sc.textFile(base_path + basket_path).map(x => x.split(" "))

    // 2. 使用FP-Growth算法生成频繁模式
    val fpg = new FPGrowth().setMinSupport(min_sup).setNumPartitions(10)
    val model = fpg.run(basket)
    // 2.1 生成频繁模式
    model.freqItemsets.collect()
    model.freqItemsets.saveAsTextFile(base_path + pattern_path)
//      .foreach{itemset => println(s"${itemset.items.mkString("[", ",", "]")},${itemset.freq}")}
    // 2.2 生成关联规则
    val minConfidence = 0.2
    val Rules = model.generateAssociationRules(minConfidence).collect()

    // 4. 关联规则匹配
    println("Matching Association Rules ...")
    // 4.1 读取用户数据
    val user = sc.textFile(base_path + user_path)
    // 4.2 匹配
    val res = user.collect().map(x => {
      val his = x.split(" ")
      var max_conf: Double = 0
      var item: String = "0"

      // 遍历Rules查找所有的规则，并保留符合条件的最大置信度的商品
      Rules.foreach(rule => {
        // 不包含被推荐物品
        if(!his.contains(rule.consequent(0))){
          // 前置商品都存在
          var flg = 0
          for(i <- rule.antecedent.indices){
            if(!his.contains(rule.antecedent(i))) flg = 1
          }
          if(flg == 0) {
            if(rule.confidence >= max_conf){
              max_conf = rule.confidence
              item = rule.consequent(0)
            }
          }
        }
      })
      item
    })
    sc.parallelize(res).saveAsTextFile(base_path + res_path)

    println("Main End ============================")

    sc.stop()
  }
}